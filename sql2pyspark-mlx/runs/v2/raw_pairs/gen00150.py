from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "desktop") | ~F.col("device").isin(["tablet", "mobile", "desktop"]))
    .select(
        "id",
        "country",
        "browser",
        "started_at",
        F.weekofyear("started_at").alias("started_at_weekofyear"),
    )
)
