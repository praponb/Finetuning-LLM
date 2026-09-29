from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Chrome", "Firefox"]) & F.hour("started_at").between(7, 15))
    .select(
        "id",
        "started_at",
        "browser",
        F.coalesce("country", F.lit("Unknown")).alias("country_filled"),
    )
)
