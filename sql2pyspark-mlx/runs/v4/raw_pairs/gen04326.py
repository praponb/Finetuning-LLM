from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 204, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.stddev_samp("quantity").alias("quantity_stddev_2"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
)
