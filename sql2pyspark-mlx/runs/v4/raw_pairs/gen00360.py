from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(~((F.col("quantity") == 4) | (F.col("quantity") < 5)))
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
)

agg = (
    valid_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        (F.max("unit_price_int") - F.min("unit_price_int")).alias("unit_price_int_range"),
    )
)

result = (
    agg
    .select("unit_price_bucket", "unit_price_int_range")
    .orderBy(F.col("unit_price_int_range").desc())
)
