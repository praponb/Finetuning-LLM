from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("revenue") <= 2100)

agg = (
    src
    .groupBy("product_id", "store_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
)

result = (
    agg
    .orderBy(F.col("store_id").desc(), F.col("product_id").desc())
    .limit(50)
)
