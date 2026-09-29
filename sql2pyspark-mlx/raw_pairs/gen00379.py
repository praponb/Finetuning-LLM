from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .select("full_name", F.lit("employee").alias("kind"))
    .union(
        df_sessions
        .filter(F.col("browser").isNotNull())
        .select("browser", F.lit("session").alias("kind"))
    )
    .distinct()
)
