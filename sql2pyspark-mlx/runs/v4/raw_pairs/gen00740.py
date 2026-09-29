from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") >= 23)
    .groupBy(F.month("sale_date").alias("sale_month"), "store_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
    .orderBy(F.col("store_id").desc())
)
