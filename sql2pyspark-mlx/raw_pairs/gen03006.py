from pyspark.sql import functions as F

kept_keys = (
    df_subscriptions
    .select("status", "id")
    .exceptAll(
        df_subscriptions
        .filter(F.month("end_date").isin([3, 4, 6]))
        .select("status", "id")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "plan", "start_date", "end_date", "status")
    .orderBy("plan", "start_date")
)
