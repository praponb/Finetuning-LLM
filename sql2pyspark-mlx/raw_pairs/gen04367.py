from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("device", "browser")
    .exceptAll(
        df_sessions
        .filter(~F.col("browser").isin(["Chrome", "Firefox", "Safari"]))
        .select("device", "browser")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("week", "started_at").alias("week_start"))
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.max("started_at").alias("last_started_at"),
    )
    .filter(F.col("th_count") > 1)
)
