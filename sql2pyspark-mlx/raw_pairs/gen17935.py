from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(-5, -1)
w_running = Window.partitionBy(F.month("signup_date"), "country").orderBy("signup_date", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("segment", "city").orderBy(F.col("city_count_credit_limit").desc_nulls_first(), "id")
w5 = Window.partitionBy("country").orderBy("city_count_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.month("signup_date"), "city").orderBy(F.col("age_avg_5_prior").desc_nulls_first())
w7 = Window.partitionBy("country").orderBy("age_avg_5_prior_cume_dist", "id")

metrics = (
    df_customers
    .withColumn("city_count_credit_limit", F.count("credit_limit").over(w))
    .withColumn("age_avg_5_prior", F.avg("age").over(w2))
    .withColumn("credit_limit_avg_5", F.avg("credit_limit").over(w_running))
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    ranked
    .filter(F.col("city").isNotNull() & (F.col("credit_limit") < 13100))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "city_count_credit_limit",
        "age_avg_5_prior",
        "credit_limit_avg_5",
        "dedup_rank",
        F.substring("segment", 1, 2).alias("segment_prefix"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_filled"),
    )
)

metrics_customers = (
    cleaned
    .select(
        "*",
        F.last("credit_limit_avg_5", ignorenulls=True).over(w5).alias("last_known_credit_limit_avg_5"),
        F.cume_dist().over(w6).alias("age_avg_5_prior_cume_dist"),
    )
)

eligible = (
    metrics_customers
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.device") != "mobile"), "left_semi")
)

deduped = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "age",
        "city_count_credit_limit",
        "age_avg_5_prior",
        "credit_limit_avg_5",
        "dedup_rank",
        "segment_prefix",
        "segment_filled",
        "last_known_credit_limit_avg_5",
        "age_avg_5_prior_cume_dist",
    )
)

matched_keys = (
    deduped
    .select("segment", "country")
    .subtract(
        df_customers
        .filter(F.col("credit_limit") <= 9500)
        .select("segment", "country")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "age",
        "age_avg_5_prior",
        "segment_prefix",
        "last_known_credit_limit_avg_5",
    )
    .orderBy("age")
)
