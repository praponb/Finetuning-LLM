from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.year("signup_date"), "segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment").orderBy("age_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "session_country").orderBy("credit_limit", "id")
w5 = Window.partitionBy("segment").orderBy(F.col("event_seq").desc())

eligible = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.customer_id").isin([12, 36])), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.signup_date")), "left_anti")
)

base = (
    kept
    .filter((F.col("signup_date") >= F.lit("2023-03-22").cast("date")) & F.col("segment").isNotNull())
    .withColumn("age_scaled", F.round((F.col("age") * 1.15), 2))
    .withColumn("resolved_city", F.coalesce("city", F.lit("UNKNOWN")))
)

latest_match = (
    base
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country").alias("session_country"))
)

scored = (
    latest_match
    .filter(F.year("signup_date") == 2024)
    .select(
        "id",
        "name",
        "segment",
        "is_active",
        "resolved_city",
        F.last("session_country", ignorenulls=True).over(w2).alias("last_known_session_country"),
        F.sum("age_scaled").over(w3).alias("running_age_scaled"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy("segment")
    .agg(
        F.round(F.sum("running_age_scaled") / F.nullif(F.count(F.when(F.col("segment") == "Corporate", 1)), F.lit(0)), 2).alias("running_age_scaled_per_corporate"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("event_seq")).otherwise(0)).alias("smb_event_seq"),
        F.percentile_approx("running_age_scaled", [0.25, 0.5, 0.75]).alias("running_age_scaled_quartiles"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") < 1000)
    .orderBy(F.col("smb_event_seq").desc())
)
