from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNotNull())
    .select(F.col("cost").alias("value"))
    .unionByName(
        df_transactions
        .filter(F.col("amount").isNotNull())
        .select(F.col("amount").alias("value"))
    )
)
