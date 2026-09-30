from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .union(df_store_sales.select("sale_date"))
)
