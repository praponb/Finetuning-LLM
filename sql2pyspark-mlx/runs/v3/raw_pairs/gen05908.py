from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([24, 32]))
    .select("price")
    .union(df_orders.filter(F.col("customer_id") == 1).select("amount"))
    .distinct()
)
