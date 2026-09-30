from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount", F.lit("transaction").alias("source"))
    .union(
        df_products
        .filter(F.col("product_name").isNotNull())
        .select("price", F.lit("product").alias("source"))
    )
    .distinct()
    .orderBy(F.col("amount").desc())
)
