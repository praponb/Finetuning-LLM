from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee", F.lit("order").alias("source"))
    .union(
        df_products
        .filter(F.col("stock_qty") >= 267)
        .select("price", F.lit("product").alias("source"))
    )
    .orderBy("source")
)
