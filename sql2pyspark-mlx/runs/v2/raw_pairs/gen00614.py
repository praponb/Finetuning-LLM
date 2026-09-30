from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("unit_price") >= 140)
    .withColumn("quantity_rounded", F.round("quantity", 1))
)

summary = (
    src
    .groupBy("product_id", "order_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
)

result = (
    summary
    .select("product_id", "order_id")
    .orderBy(F.col("order_id").desc(), "product_id")
    .limit(5)
)
