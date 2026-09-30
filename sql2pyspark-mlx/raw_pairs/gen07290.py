from pyspark.sql import functions as F

remaining_keys = (
    df_subscriptions
    .select("id", "plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("plan") == "enterprise")
        .select("id", "plan")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "plan", "end_date", "monthly_fee")
    .orderBy(F.col("end_date").desc_nulls_last())
)
