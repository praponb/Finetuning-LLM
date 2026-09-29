from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .subtract(df_store_sales.select(F.col("sale_date").alias("opened_date")))
)
