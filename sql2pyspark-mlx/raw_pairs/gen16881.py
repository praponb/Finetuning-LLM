from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "Singapore")
    .select("opened_date")
    .union(df_store_sales.select("sale_date"))
    .distinct()
)
