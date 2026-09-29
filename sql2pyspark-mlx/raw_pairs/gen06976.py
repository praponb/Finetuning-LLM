from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .select(F.col("full_name").alias("label"))
    .intersect(
        df_sessions
        .filter(F.col("device").isNotNull())
        .select(F.col("device").alias("label"))
    )
    .orderBy("label")
)
