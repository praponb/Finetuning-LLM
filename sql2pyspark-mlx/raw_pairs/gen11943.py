from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        "order_id",
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
    )
)
