from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-01-01")
    .select(F.col("amount").alias("value"))
    .subtract(
        df_products
        .filter(F.col("category") != "Home")
        .select(F.col("price").alias("value"))
    )
    .orderBy("value")
)
