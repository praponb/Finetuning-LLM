from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "started_at",
        "country",
        F.to_date("started_at").alias("started_at_date"),
    )
)
