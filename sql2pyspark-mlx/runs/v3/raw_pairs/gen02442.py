from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.31)
    .select("amount")
    .union(df_store_sales.select("revenue"))
)
