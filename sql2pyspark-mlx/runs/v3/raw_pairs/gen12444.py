from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "North")
    .select("opened_date")
    .subtract(df_store_sales.select("sale_date"))
)
