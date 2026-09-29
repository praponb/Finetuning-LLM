from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").isNotNull())
    .select(F.lit("customer").alias("source"), "signup_date")
    .union(
        df_stores
        .filter(F.col("opened_date").isNotNull())
        .select(F.lit("store").alias("source"), "opened_date")
    )
    .distinct()
)
