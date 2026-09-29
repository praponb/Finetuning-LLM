from pyspark.sql import functions as F

result = (
    df_products
    .select("price", F.lit("product").alias("source"))
    .unionByName(
        df_orders
        .select(F.col("amount").alias("price"), F.lit("order").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
