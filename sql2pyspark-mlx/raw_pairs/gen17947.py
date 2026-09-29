from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select("browser")
    .unionByName(
        df_employees
        .filter(F.col("level").isNotNull())
        .select(F.col("level").alias("browser"))
    )
)
