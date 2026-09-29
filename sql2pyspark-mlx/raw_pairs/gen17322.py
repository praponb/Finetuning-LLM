from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.lit("payment").alias("kind"), "status")
    .unionByName(
        df_customers
        .select(F.lit("customer").alias("kind"), F.col("city").alias("status"))
    )
    .distinct()
)
