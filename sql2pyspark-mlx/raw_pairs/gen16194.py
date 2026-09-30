from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("credit_limit").desc_nulls_first())
w3 = Window.partitionBy("segment", "city").orderBy(F.col("credit_limit").desc(), "id")

base = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .withColumn("name_normalized", F.upper(F.trim("name")))
    .withColumn("credit_limit_level", F.when(F.col("credit_limit") >= 13200, "High").when(F.col("credit_limit") >= 5200, "Medium").otherwise("Low"))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("OTHER")))
)

with_last = (
    base
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.start_date"), F.col("c.plan").alias("subscription_plan"))
)

with_windows = (
    with_last
    .select(
        "id",
        "name",
        "city",
        "signup_date",
        "is_active",
        "age",
        "name_normalized",
        "credit_limit_level",
        "resolved_segment",
        "start_date",
        F.dense_rank().over(w2).alias("credit_limit_dense_rank"),
        F.ntile(3).over(w3).alias("credit_limit_bucket"),
    )
)

filtered = (
    with_windows
    .filter(F.col("credit_limit_bucket") < 6)
    .select(
        "id",
        "name",
        "is_active",
        "age",
        "name_normalized",
        "credit_limit_level",
        "resolved_segment",
        "start_date",
        "credit_limit_dense_rank",
        "credit_limit_bucket",
        F.upper("city").alias("city_upper"),
        (F.datediff("start_date", "signup_date") / 7).alias("weeks_signup_date_to_start_date"),
    )
)

result = (
    filtered
    .groupBy(
        F.date_trunc("YEAR", "start_date").alias("year_start"),
        F.when(F.col("credit_limit_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("credit_limit_bucket_tier"),
        F.when(F.col("age") >= 65, "HIGH").otherwise("LOW").alias("age_tier"),
    )
    .agg(
        F.percentile_approx("credit_limit_dense_rank", 0.5).alias("credit_limit_dense_rank_p50"),
        F.percentile_approx("credit_limit_bucket", 0.9).alias("credit_limit_bucket_p90"),
        F.count("*").alias("event_count"),
    )
    .orderBy("credit_limit_dense_rank_p50")
)
