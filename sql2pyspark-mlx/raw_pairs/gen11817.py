from pyspark.sql import functions as F

remaining_keys = (
    df_subscriptions
    .select("customer_id")
    .exceptAll(
        df_subscriptions
        .filter(F.year("start_date") >= 2024)
        .select("customer_id")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("status").rlike("^(CANCELLED|EXPIRED)$"), "GROUP_B").when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "GROUP_A").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("end_date").asc_nulls_first())
)
