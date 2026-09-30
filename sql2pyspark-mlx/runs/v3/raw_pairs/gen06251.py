from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .union(df_transactions.select("amount"))
    .distinct()
)
