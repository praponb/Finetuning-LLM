from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount")
    .union(df_products.select("price"))
    .orderBy("amount")
)
