from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.coalesce("cost", F.lit(0)).alias("cost_filled"))
)

result = (
    valid
    .groupBy(
        "carrier",
        F.when(F.col("cost_filled") >= 85, "PREMIUM").otherwise("STANDARD").alias("cost_filled_tier"),
        F.when(F.col("cost") >= 94, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.min("cost").alias("min_cost"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_rows"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)
