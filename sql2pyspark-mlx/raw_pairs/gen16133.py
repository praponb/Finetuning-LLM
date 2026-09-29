from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isNotNull())
    .select("txn_type", F.lit("transaction").alias("origin"))
    .unionByName(
        df_products
        .filter(F.col("product_name").isNotNull())
        .select(F.col("product_name").alias("txn_type"), F.lit("product").alias("origin"))
    )
    .orderBy("txn_type")
)
