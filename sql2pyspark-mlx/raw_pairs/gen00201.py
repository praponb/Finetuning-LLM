from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "South")
    .select(F.lit("store").alias("kind"), "opened_date")
    .union(
        df_customers
        .filter(F.col("is_active"))
        .select(F.lit("customer").alias("kind"), "signup_date")
    )
)
