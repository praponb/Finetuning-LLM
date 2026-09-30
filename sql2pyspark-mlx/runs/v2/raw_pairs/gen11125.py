from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.count("*").alias("order_count"),
    )
    .filter(F.col("shipping_fee_range") >= 36)
    .orderBy(F.col("shipping_fee_range").desc(), F.col("order_count").desc())
)
