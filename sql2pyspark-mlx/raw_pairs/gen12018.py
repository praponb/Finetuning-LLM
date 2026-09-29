from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("credit_limit").desc(), "id")
w3 = Window.partitionBy("city")
w4 = Window.orderBy(F.col("age").asc_nulls_first())
w5 = Window.partitionBy("country").orderBy("end_date", "id")
w6 = Window.partitionBy(F.date_trunc("YEAR", "subscription_start_date")).orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy(F.col("last_known_city_avg_credit_limit").asc_nulls_last())
w8 = Window.orderBy("prev_city_avg_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.date_trunc("week", "subscription_start_date")).orderBy("credit_limit_bucket", "id")
w10 = Window.orderBy("prev_city_avg_credit_limit")
w11 = Window.partitionBy("activity_month").orderBy(F.col("last_known_city_avg_credit_limit_cume_dist_p90").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.start_date").alias("subscription_start_date"),
        F.col("c.end_date"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") >= F.col("s.end_date")), "left_anti")
)

metrics = (
    kept
    .select(
        "*",
        F.ntile(10).over(w2).alias("credit_limit_bucket"),
        F.avg("credit_limit").over(w3).alias("city_avg_credit_limit"),
    )
)

enriched = (
    metrics
    .filter(F.col("subscription_start_date") >= "2025-10-01")
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "age",
        "subscription_start_date",
        "end_date",
        "credit_limit_bucket",
        "city_avg_credit_limit",
        F.rank().over(w4).alias("age_rank"),
        F.lag("city_avg_credit_limit", 1).over(w5).alias("prev_city_avg_credit_limit"),
        F.last("city_avg_credit_limit", ignorenulls=True).over(w6).alias("last_known_city_avg_credit_limit"),
    )
)

metrics_customers = (
    enriched
    .select(
        "id",
        "name",
        "email",
        "age",
        "end_date",
        "credit_limit_bucket",
        "city_avg_credit_limit",
        "age_rank",
        "prev_city_avg_credit_limit",
        F.round((F.col("prev_city_avg_credit_limit") / F.sum("prev_city_avg_credit_limit").over(w3)), 4).alias("prev_city_avg_credit_limit_share"),
        F.cume_dist().over(w7).alias("last_known_city_avg_credit_limit_cume_dist"),
        F.last("age", ignorenulls=True).over(w8).alias("last_known_age"),
        F.lag("credit_limit_bucket", 1).over(w9).alias("prev_credit_limit_bucket"),
        F.percent_rank().over(w10).alias("prev_city_avg_credit_limit_percent_rank"),
    )
)

result = (
    metrics_customers
    .groupBy(
        F.date_trunc("MONTH", "end_date").alias("activity_month"),
        F.date_trunc("WEEK", "end_date").alias("week_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_credit_limit_bucket", [0.25, 0.5, 0.75]).alias("prev_credit_limit_bucket_quartiles"),
        F.percentile_approx("prev_city_avg_credit_limit", 0.9).alias("prev_city_avg_credit_limit_p90"),
        F.max("prev_city_avg_credit_limit_share").alias("max_prev_city_avg_credit_limit_share"),
        F.percentile_approx("last_known_city_avg_credit_limit_cume_dist", 0.9).alias("last_known_city_avg_credit_limit_cume_dist_p90"),
    )
    .withColumn("last_known_city_avg_credit_limit_cume_dist_p90_percent_rank", F.percent_rank().over(w11))
    .orderBy(F.col("max_prev_city_avg_credit_limit_share").desc_nulls_first())
)
