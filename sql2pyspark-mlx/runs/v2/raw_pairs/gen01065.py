from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
    .orderBy(F.col("last_sale_date").desc())
)
