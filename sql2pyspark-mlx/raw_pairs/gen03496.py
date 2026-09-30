from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("location")
    .subtract(
        df_transactions
        .filter(F.col("merchant").isNotNull())
        .select(F.col("merchant").alias("location"))
    )
)
