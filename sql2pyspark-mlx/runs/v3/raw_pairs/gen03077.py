from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(~(F.col("store_id").isin([2, 3, 4]) & (F.col("sale_date") > "2025-05-01")))
)

agg = (
    src
    .groupBy("product_id")
    .agg(F.countDistinct("revenue").alias("unique_products"))
)

result = (
    agg
    .filter(F.col("unique_products") > 10)
    .select("product_id", "unique_products")
    .orderBy(F.col("unique_products").desc(), F.col("product_id").desc())
)
