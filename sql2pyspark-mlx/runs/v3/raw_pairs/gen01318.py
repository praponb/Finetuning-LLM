from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "store_id", F.date_sub("sale_date", 19).alias("sale_date_minus_19d"))
    .orderBy("sale_date_minus_19d")
)
