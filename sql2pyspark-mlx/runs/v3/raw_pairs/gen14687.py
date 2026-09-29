from pyspark.sql import functions as F

src = df_order_items.filter(~(F.col("product_id").isNull() & (F.col("quantity") == 3)))

agg = (
    src
    .groupBy("product_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

result = (
    agg
    .filter(F.col("avg_unit_price") > 160)
    .select("product_id", "avg_unit_price")
    .orderBy(F.col("product_id").desc())
)
