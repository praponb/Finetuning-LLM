from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .select(F.col("revenue").alias("value"))
    .unionByName(
        df_payments
        .filter(F.col("amount").isNotNull())
        .select(F.col("amount").alias("value"))
    )
)
