from pyspark.sql import functions as F

base = (
    df_order_items
    .filter((F.col("order_id").isin([1, 6, 18, 35]) & (F.col("unit_price") <= 280)) | F.col("product_id").isin([8, 20, 30, 38]))
)

agg = (
    base
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    agg
    .filter(F.col("total_unit_price") >= 1200)
    .orderBy(F.col("total_unit_price").desc(), "product_id")
    .limit(5)
)
