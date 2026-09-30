from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("is_mobile").desc_nulls_last(), "id")

base = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (F.col("country") == "US"))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.when(F.col("device") == "mobile", 1).otherwise(0).alias("is_mobile"),
        (F.datediff(F.current_date(), "started_at") / 365.25).alias("years_since_started_at"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_anti")
)

ranked = (
    kept
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "browser", "is_mobile", "years_since_started_at")
)

eligible = (
    ranked
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < "2024-06-12"), "left_anti")
)

matched_keys = (
    eligible
    .select("id", "customer_id")
    .subtract(
        df_sessions
        .filter(F.col("browser").isin(["Safari", "Edge"]))
        .select("id", "customer_id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("browser", "is_mobile", "years_since_started_at")
    .orderBy(F.col("browser").desc_nulls_first())
)
