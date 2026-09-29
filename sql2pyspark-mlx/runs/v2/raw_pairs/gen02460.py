from pyspark.sql import functions as F

src = df_store_sales.filter(F.month("sale_date").isin([3, 9, 10]))

agg = (
    src
    .groupBy("product_id", "store_id")
    .agg(F.countDistinct("revenue").alias("unique_stores"))
)

result = (
    agg
    .filter(F.col("unique_stores") >= 8)
    .select("product_id", "store_id", "unique_stores")
    .orderBy(F.col("store_id").desc())
)
