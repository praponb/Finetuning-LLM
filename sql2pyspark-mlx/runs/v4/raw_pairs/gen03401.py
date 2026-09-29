from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("order_id", "status")
w3 = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("status").orderBy(F.col("days_delivered_date_to_shipped_date").desc_nulls_last())
w5 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w6 = Window.partitionBy("status").orderBy(F.col("cost_scaled").desc_nulls_first())

filtered = (
    df_shipments
    .filter(F.col("delivered_date") >= "2025-04-01")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

metrics = (
    filtered
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
    .withColumn("cost_share_2", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
)

filtered_shipments = (
    metrics
    .filter(F.col("delivered_date") > F.lit("2023-11-08").cast("date"))
    .withColumn("cost_scaled", F.round((F.col("cost") * 0.9), 2))
)

with_windows = (
    filtered_shipments
    .withColumn("cost_share_avg_6_prior", F.avg("cost_share").over(w3))
    .withColumn("days_delivered_date_to_shipped_date_rank", F.rank().over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("cost_scaled_cume_dist", F.cume_dist().over(w6))
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        "days_delivered_date_to_shipped_date",
        "resolved_carrier",
        "cost_share",
        "cost_share_2",
        "cost_scaled",
        F.when(F.col("status").rlike("^(LOST|IN_TRANSIT)$"), "CORE").when(F.col("status").rlike("^(DELIVERED|RETURNED)$"), "GROUP_B").otherwise("PRIORITY").alias("status_zone"),
    )
)
