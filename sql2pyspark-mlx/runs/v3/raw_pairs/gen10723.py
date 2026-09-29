from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "revenue", F.last_day("sale_date").alias("sale_date_month_end"))
    .orderBy("sale_date_month_end")
)
