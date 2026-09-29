from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .union(df_orders.filter(F.year("order_date") == 2025).select("amount"))
    .distinct()
    .orderBy(F.col("price").desc())
)
