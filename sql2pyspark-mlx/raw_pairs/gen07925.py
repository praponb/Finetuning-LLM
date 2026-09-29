from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("age", "id")
w4 = Window.partitionBy("segment", "city").orderBy(F.col("credit_limit").desc_nulls_last(), "id")
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("city").orderBy("last3_sum_running_min_age", "id").rowsBetween(-3, -1)

cleaned = (
    df_customers
    .filter((F.col("segment").eqNullSafe(F.lit("Corporate"))) & F.col("city").isNotNull())
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
    .withColumn("email_domain", F.regexp_extract("email", "@(.+)$", 1))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("N/A")))
)

enriched = (
    cleaned
    .filter(F.col("segment") == "Consumer")
    .withColumn("running_avg_age", F.avg("age").over(w))
    .withColumn("running_min_age", F.min("age").over(w))
    .withColumn("running_sum_age", F.sum("age").over(w))
    .withColumn("running_count_age", F.count("age").over(w))
    .withColumn("last_known_credit_limit", F.last("credit_limit", ignorenulls=True).over(w))
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("running_min_age").over(w2).alias("last3_sum_running_min_age"),
        F.avg("running_min_age").over(w2).alias("last3_avg_running_min_age"),
        F.count("running_min_age").over(w2).alias("last3_count_running_min_age"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "email",
        "city",
        "signup_date",
        "age",
        "email_domain",
        "resolved_segment",
        "running_avg_age",
        "running_min_age",
        "running_count_age",
        "last_known_credit_limit",
        "last3_sum_running_min_age",
        "last3_avg_running_min_age",
        "last3_count_running_min_age",
        "row_seq",
    )
)

with_windows = (
    deduped
    .select(
        "id",
        "signup_date",
        "email_domain",
        "running_min_age",
        "last_known_credit_limit",
        "last3_sum_running_min_age",
        "row_seq",
        F.round((F.col("age") / F.sum("age").over(w5)), 4).alias("age_share"),
        F.avg("last3_avg_running_min_age").over(w6).alias("last3_avg_running_min_age_avg_3_prior"),
    )
)

stats = (
    with_windows
    .groupBy(F.date_trunc("QUARTER", "signup_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("last_known_credit_limit", [0.25, 0.5, 0.75]).alias("last_known_credit_limit_quartiles"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 1)
)

result = (
    stats
    .select("quarter_start", "num_rows", "last_known_credit_limit_quartiles", "cnt")
    .orderBy(F.col("cnt").desc())
)
