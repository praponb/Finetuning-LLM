from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("quantity").between(3, 6))
    .withColumn("unit_price_whole", F.floor("unit_price"))
)

result = (
    filtered
    .groupBy("order_id")
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
)
