from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.23, 0.25) & F.col("channel").isin(["web", "mobile"]))
    .groupBy("store_id", "status")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.count("discount").alias("discount_count"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
    .filter(F.col("discount_range") >= 67)
    .orderBy(F.col("discount_range").desc())
)
