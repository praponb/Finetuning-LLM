from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("customer_id", "country")
    .subtract(
        df_sessions
        .filter(F.col("browser") != "Safari")
        .select("customer_id", "country")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = kept_rows.select("id", "device", "browser", "started_at", "country")
