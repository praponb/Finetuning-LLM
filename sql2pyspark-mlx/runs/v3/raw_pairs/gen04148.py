from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount")
    .subtract(df_products.filter(F.col("price") <= 540).select("price"))
)
