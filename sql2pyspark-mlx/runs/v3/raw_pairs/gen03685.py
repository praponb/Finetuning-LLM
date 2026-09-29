from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount")
    .union(df_store_sales.select("revenue"))
    .distinct()
)
