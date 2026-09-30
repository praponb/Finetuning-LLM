from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")
w_recent = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("prev_days_shipped_date_to_delivered_date").asc_nulls_last(), "id")

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        F.upper("carrier").alias("carrier_upper"),
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select("s.*", F.col("d.status").alias("order_status"))
)

with_windows = (
    with_dims
    .filter(F.col("days_shipped_date_to_delivered_date") <= 63)
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        "carrier_upper",
        "days_shipped_date_to_delivered_date",
        "order_status",
        F.lag("days_shipped_date_to_delivered_date", 1).over(w).alias("prev_days_shipped_date_to_delivered_date"),
        F.ntile(3).over(w_recent).alias("cost_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

cleaned_shipments = (
    latest
    .filter(F.col("status").isin(["DELIVERED", "IN_TRANSIT", "RETURNED"]))
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        "carrier_upper",
        "order_status",
        "cost_bucket",
        F.when(F.col("status") == "RETURNED", 1).otherwise(0).alias("is_returned"),
        F.coalesce("order_status", F.lit("DIRECT")).alias("order_status_clean"),
    )
)

result = (
    cleaned_shipments
    .groupBy("order_status")
    .pivot("status", ["LOST", "RETURNED", "DELIVERED"])
    .agg(F.avg("cost_bucket").alias("amount"))
    .select(
        "order_status",
        F.col("LOST").alias("lost"),
        F.col("RETURNED").alias("returned"),
        F.col("DELIVERED").alias("delivered"),
    )
)
