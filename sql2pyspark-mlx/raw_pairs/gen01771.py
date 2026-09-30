from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNotNull())
    .select("cost", F.lit("product").alias("origin"))
    .union(
        df_orders
        .filter(F.col("amount").isNotNull())
        .select("amount", F.lit("order").alias("origin"))
    )
    .orderBy("cost")
)
