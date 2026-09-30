from pyspark.sql import functions as F

kept_keys = (
    df_sessions
    .select("device", "customer_id")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("device", "customer_id")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = kept_rows.select("id", "started_at", "country")
