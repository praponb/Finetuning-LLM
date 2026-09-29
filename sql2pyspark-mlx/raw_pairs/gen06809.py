from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue", F.lit("store_sale").alias("origin"))
    .union(df_orders.select("amount", F.lit("order").alias("origin")))
)
