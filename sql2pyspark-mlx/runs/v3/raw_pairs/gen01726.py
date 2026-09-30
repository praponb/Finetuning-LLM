from pyspark.sql import functions as F

result = (
    df_transactions
    .select(F.col("amount").alias("value"))
    .unionByName(
        df_products
        .filter(F.col("price") > 1100)
        .select(F.col("price").alias("value"))
    )
    .distinct()
    .orderBy("value")
)
