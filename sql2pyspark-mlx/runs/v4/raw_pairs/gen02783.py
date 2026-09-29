from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("country", "customer_id")
    .subtract(
        df_sessions
        .filter(F.col("browser") == "Safari")
        .select("country", "customer_id")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("browser").rlike("^(Chrome|Firefox)$"), "PRIORITY").otherwise("GROUP_B").alias("browser_zone"),
    )
    .orderBy(F.col("browser_zone").desc())
)
