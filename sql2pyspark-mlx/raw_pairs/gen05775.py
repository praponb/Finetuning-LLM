from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .select("amount")
    .intersect(
        df_products
        .filter(F.col("cost").isNotNull())
        .select(F.col("cost").alias("amount"))
    )
)
