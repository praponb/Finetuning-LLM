from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Electronics")
    .select(F.col("cost").alias("value"), F.lit("product").alias("origin"))
    .unionByName(
        df_transactions
        .select(F.col("amount").alias("value"), F.lit("transaction").alias("origin"))
    )
)
