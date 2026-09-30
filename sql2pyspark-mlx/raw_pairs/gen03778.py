from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .select("email")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan").isNotNull())
        .select(F.col("plan").alias("email"))
    )
    .distinct()
    .orderBy(F.col("email").desc())
)
