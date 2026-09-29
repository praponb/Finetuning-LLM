from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "started_at",
        "browser",
        F.weekofyear("started_at").alias("started_at_weekofyear"),
    )
)
