from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("carrier").orderBy("cost_adjusted", "id")
w_running = Window.partitionBy("order_id").orderBy("least_cost_cost_adjusted", "id")
w_ordered = Window.orderBy("shipped_date", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "LOST", "IN_TRANSIT"]))
    .withColumn("cost_adjusted", F.round((F.col("cost") * 0.9), 2))
    .withColumn("carrier_upper", F.upper("carrier"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("UNKNOWN")))
)

valid = (
    filtered
    .filter(F.col("shipped_date") > F.to_date(F.lit("2023-09-10")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost_adjusted",
        "carrier_upper",
        "resolved_carrier",
        F.least("cost", "cost_adjusted").alias("least_cost_cost_adjusted"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_filled"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .withColumn("least_cost_cost_adjusted_bucket", F.ntile(3).over(w_running))
    .withColumn("cost_adjusted_avg_2", F.avg("cost_adjusted").over(w_ordered))
    .withColumn("prev_least_cost_cost_adjusted", F.lag("least_cost_cost_adjusted", 1).over(w_running))
)
