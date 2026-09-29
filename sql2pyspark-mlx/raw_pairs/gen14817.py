from pyspark.sql import functions as F

kept_keys = (
    df_sessions
    .select("customer_id")
    .intersectAll(
        df_sessions
        .filter(F.col("browser").isin(["Chrome", "Firefox"]))
        .select("customer_id")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "CORE").otherwise("GROUP_A").alias("device_zone"),
    )
    .orderBy(F.col("device_zone").desc(), "customer_id")
)
