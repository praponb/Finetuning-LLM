from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").isNotNull())
    .select("amount")
    .subtract(
        df_products
        .filter(F.col("price").isNotNull())
        .select(F.col("price").alias("amount"))
    )
    .orderBy("amount")
)
