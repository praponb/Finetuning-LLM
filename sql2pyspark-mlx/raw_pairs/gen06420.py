from pyspark.sql import functions as F

result = (
    df_products
    .select("product_name", F.lit("product").alias("source"))
    .union(
        df_orders
        .filter(F.col("status") != "SHIPPED")
        .select("channel", F.lit("order").alias("source"))
    )
    .distinct()
)
