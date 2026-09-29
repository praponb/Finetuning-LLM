from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
    .orderBy(F.col("store_id").desc(), "product_id")
    .limit(1)
)
