from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("order_id").isin([17, 31, 34]) & F.col("unit_price").isNotNull()))
    .groupBy("product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.count("*").alias("num_order_items"),
    )
    .filter(F.col("unit_price_range") < 66)
)
