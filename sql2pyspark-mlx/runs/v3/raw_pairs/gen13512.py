from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount")
    .union(df_products.select("cost"))
    .distinct()
    .orderBy(F.col("amount").desc())
)
