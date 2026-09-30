from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([9, 26]))
    .select(F.lit("order").alias("source"), "shipping_fee")
    .union(df_store_sales.select(F.lit("store_sale").alias("source"), "revenue"))
    .distinct()
)
