from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Sports")
    .select("price")
    .union(df_transactions.select("amount"))
    .distinct()
)
