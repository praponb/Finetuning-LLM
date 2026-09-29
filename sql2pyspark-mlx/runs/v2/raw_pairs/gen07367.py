from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
    .orderBy(F.col("store_id").desc())
)
