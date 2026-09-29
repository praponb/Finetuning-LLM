from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter(((F.col("quantity") != 6) & F.col("order_id").isin([12, 23, 26])) | (F.col("unit_price") < 290))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

agg = (
    filtered_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("unit_price").alias("max_unit_price"),
    )
)

result = (
    agg
    .filter(F.col("max_unit_price") > 320)
    .select("unit_price_bucket", "order_id", "quantity_range", "max_unit_price")
)
