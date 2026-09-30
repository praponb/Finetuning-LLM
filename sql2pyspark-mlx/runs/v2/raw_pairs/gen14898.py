from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") != "TH")
    .select(
        "id",
        "browser",
        F.date_trunc("week", "started_at").alias("started_at_period"),
    )
)
