from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["London", "Chiang Mai", "New York"]))
    .select(F.lit("store").alias("source"), F.col("opened_date").alias("event_date"))
    .unionByName(
        df_customers
        .filter(F.col("segment").isNotNull())
        .select(F.lit("customer").alias("source"), F.col("signup_date").alias("event_date"))
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
