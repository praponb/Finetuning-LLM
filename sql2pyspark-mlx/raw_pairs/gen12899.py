from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("signup_date_plus_months", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("subscription_plan").orderBy("is_active_flag", "id")
w4 = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit").desc_nulls_last())

with_last = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.plan").alias("subscription_plan"))
)

cleaned = (
    with_last
    .filter((F.col("city") == "Berlin") & (~F.col("city").eqNullSafe(F.lit("Berlin"))))
    .select(
        "*",
        F.col("is_active").cast("int").alias("is_active_flag"),
        F.add_months("signup_date", 12).alias("signup_date_plus_months"),
        F.coalesce("segment", F.lit("OTHER")).alias("resolved_segment"),
    )
)

metrics = (
    cleaned
    .filter(F.col("is_active_flag").between(41, 73))
    .select(
        "*",
        F.count("age").over(w2).alias("last3_count_age"),
        F.sum("age").over(w2).alias("last3_sum_age"),
        F.max("age").over(w2).alias("last3_max_age"),
        F.min("age").over(w2).alias("last3_min_age"),
    )
)

valid = (
    metrics
    .filter(F.col("segment") == "Corporate")
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "subscription_plan",
        "is_active_flag",
        "signup_date_plus_months",
        "resolved_segment",
        "last3_count_age",
        "last3_sum_age",
        "last3_min_age",
        F.when(F.col("city") == "London", 1).otherwise(0).alias("is_london"),
        F.coalesce("subscription_plan", F.lit("NONE")).alias("subscription_plan_clean"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.lag("last3_min_age").over(w3).alias("prev_last3_min_age"),
        F.lead("credit_limit").over(w3).alias("next_credit_limit"),
        F.row_number().over(w3).alias("row_seq"),
        F.percent_rank().over(w4).alias("credit_limit_percent_rank"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.signup_date")), "left_semi")
)

result = (
    kept
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .withColumn("last3_sum_age_tier", F.when(F.col("last3_sum_age") >= 287, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["row_seq_tier", "activity_month", "last3_sum_age_tier"], ["row_seq_tier", "activity_month"], ["row_seq_tier"], []],
        "row_seq_tier",
        "activity_month",
        "last3_sum_age_tier",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.avg("last3_min_age").alias("avg_last3_min_age"),
        F.round(F.sum("last3_sum_age") / F.nullif(F.count(F.when(F.col("subscription_plan") == "standard", 1)), F.lit(0)), 2).alias("last3_sum_age_per_standard"),
    )
)
