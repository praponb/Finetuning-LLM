from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_part = Window.partitionBy("city", "segment").orderBy(F.col("age").desc_nulls_last())
w3 = Window.partitionBy(F.date_trunc("month", "signup_date"), "country").orderBy(F.col("corr_age_credit_limit").desc())

remaining_keys = (
    df_customers
    .select("city")
    .subtract(
        df_customers
        .filter(F.upper(F.trim("email")) == "GRACE.KIM@GMAIL.COM")
        .select("city")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(F.col("city").isNotNull())
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w))
)

kept = (
    enriched
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.browser") == "Safari"), "left_semi")
)

metrics = (
    kept
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.dense_rank().over(w_part).alias("age_dense_rank"),
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.dense_rank().over(w3).alias("corr_age_credit_limit_dense_rank"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"))
    .agg(
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.percentile_approx("corr_credit_limit_age", 0.95).alias("corr_credit_limit_age_p95"),
    )
    .orderBy(F.col("tokyo_events").desc())
)
