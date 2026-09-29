from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1100)
    .select("sale_date")
    .union(df_accounts.select("opened_date"))
    .distinct()
)
