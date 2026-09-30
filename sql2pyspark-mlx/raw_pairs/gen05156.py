from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("month", "signup_date"), "country").orderBy("credit_limit", "id")
w3 = Window.partitionBy("city").orderBy("credit_limit")
w4 = Window.orderBy(F.col("credit_limit_bucket").desc_nulls_last())
w5 = Window.partitionBy("segment").orderBy("credit_limit_percent_rank", "id")
w6 = Window.partitionBy("segment")
w7 = Window.partitionBy("segment").orderBy(F.col("credit_limit_percent_rank").asc_nulls_last(), "id")

scored = (
    df_customers
    .filter(~F.col("country").isin(["TH", "DE", "UK"]))
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(10).over(w2).alias("credit_limit_bucket"),
        F.percent_rank().over(w3).alias("credit_limit_percent_rank"),
    )
)

base = (
    scored
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "segment",
        "signup_date",
        "age",
        "row_seq",
        "credit_limit_bucket",
        "credit_limit_percent_rank",
        (F.col("credit_limit_bucket") * F.col("credit_limit_percent_rank")).alias("credit_limit_bucket_times_credit_limit_percent_rank"),
        F.concat("city", F.lit(" - "), "country").alias("city_country"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_filled"),
    )
)

base_customers = (
    base
    .filter((F.col("signup_date") > F.lit("2023-04-19").cast("date")) & (F.col("credit_limit_bucket_times_credit_limit_percent_rank") < 71))
    .withColumn("name_upper", F.upper("name"))
    .withColumn("least_age_credit_limit_bucket", F.least("age", "credit_limit_bucket"))
)

metrics = (
    base_customers
    .select(
        "*",
        F.rank().over(w4).alias("credit_limit_bucket_rank"),
        F.lag("row_seq", 1).over(w5).alias("prev_row_seq"),
        F.round((F.col("age") / F.sum("age").over(w6)), 4).alias("age_share"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .withColumn("prev_row_seq_tier", F.when(F.col("prev_row_seq") >= 9, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["prev_row_seq_tier", "segment"], ["prev_row_seq_tier"], []],
        "prev_row_seq_tier",
        "segment",
    )
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", F.col("least_age_credit_limit_bucket")).otherwise(0)).alias("consumer_least_age_credit_limit_bucket"),
        F.percentile_approx("credit_limit_bucket_rank", [0.25, 0.5, 0.75]).alias("credit_limit_bucket_rank_quartiles"),
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
        F.size(F.collect_set("segment")).alias("num_segments"),
    )
    .filter(F.col("num_segments") > 1)
)
