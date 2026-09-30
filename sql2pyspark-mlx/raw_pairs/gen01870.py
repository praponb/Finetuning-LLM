from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(F.lit("transaction").alias("origin"), "merchant")
    .unionByName(
        df_customers
        .filter(F.col("city").isNotNull())
        .select(F.lit("customer").alias("origin"), F.col("city").alias("merchant"))
    )
    .distinct()
)
