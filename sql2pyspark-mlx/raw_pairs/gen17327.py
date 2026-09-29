from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy(F.col("days_since_signup_date").desc(), "id")
w2 = Window.partitionBy("city").orderBy("signup_date", "id")
w3 = Window.orderBy("days_since_signup_date", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-06-08")))
    .withColumn("days_since_signup_date", F.datediff(F.current_date(), "signup_date"))
    .withColumn("days_since_signup_date_2", F.datediff(F.current_date(), "signup_date"))
)

kept = (
    valid
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") >= F.col("s.signup_date")), "left_anti")
)

ranked = (
    kept
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

cleaned = (
    ranked
    .filter(F.col("city").isNotNull())
    .withColumn("days_since_signup_date_2_tier", F.when(F.col("days_since_signup_date_2") >= 76, "High").when(F.col("days_since_signup_date_2") >= 23, "Medium").otherwise("Low"))
    .withColumn("segment_country", F.concat("segment", F.lit(" - "), "country"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.lag("days_since_signup_date").over(w2).alias("prev_days_since_signup_date"),
        F.lead("credit_limit").over(w2).alias("next_credit_limit"),
        F.lag("credit_limit").over(w2).alias("prev_credit_limit"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.monthly_fee"),
        F.col("c.end_date").alias("subscription_end_date"),
    )
)

kept_customers = (
    with_last
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") >= F.col("s.signup_date")), "left_anti")
)

result = (
    kept_customers
    .groupBy("country")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .orderBy(F.col("tokyo_count").desc())
)
