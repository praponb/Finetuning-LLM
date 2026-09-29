from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "level",
        "k",
        "value",
        F.when(F.col("level").rlike("^(DEBUG|INFO)$"), "PRIORITY").otherwise("CORE").alias("level_zone"),
    )
    .orderBy("id")
)
