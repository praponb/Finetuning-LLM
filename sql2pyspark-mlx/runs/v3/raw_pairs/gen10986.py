from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount", F.lit("order").alias("source"))
    .union(df_products.select("price", F.lit("product").alias("source")))
)
