from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("order_id") == 40))
    .select(
        "*",
        F.substring("carrier", 1, 2).alias("carrier_prefix"),
        F.upper("status").alias("status_upper"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

result = (
    filtered
    .groupBy("order_id", "carrier")
    .pivot("status", ["IN_TRANSIT", "RETURNED"])
    .agg(F.max("cost").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
