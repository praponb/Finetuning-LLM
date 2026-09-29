from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2023-08-26")
    .select("id", "store_id", F.date_sub("sale_date", 37).alias("sale_date_minus_37d"))
)
