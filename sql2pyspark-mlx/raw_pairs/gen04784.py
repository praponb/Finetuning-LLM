from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc(), "id")
w3 = Window.orderBy("start_date", "id")
w4 = Window.partitionBy("customer_id").orderBy("signup_date", "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")
w_grp = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.orderBy("seq_no", "id")
w8 = Window.partitionBy(F.year("start_date"))
w9 = Window.orderBy(F.col("event_seq_3").desc(), "id")
w10 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-06-07").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.lower("plan").alias("plan_lower"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "start_date", "end_date", "plan_lower")
)

enriched = (
    deduped
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)

enriched_subscriptions = (
    enriched
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select("src.id", "src.customer_id", "src.start_date", F.col("b.signup_date"))
)

base = (
    enriched_subscriptions
    .filter((F.col("start_date") > F.lit("2023-05-29").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("start_date_minus_40d", F.date_sub("start_date", 40))
)

scored = (
    base
    .select(
        "id",
        "start_date",
        "signup_date",
        "start_date_minus_40d",
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("event_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
        F.row_number().over(w5).alias("seq_no"),
        F.row_number().over(w4).alias("event_seq_3"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.first("event_seq", ignorenulls=True).over(w_grp).alias("first_known_event_seq"),
        F.ntile(5).over(w_grp_2).alias("seq_no_bucket"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w8)), 4).alias("event_seq_share"),
        F.ntile(4).over(w9).alias("event_seq_3_bucket"),
        F.last("event_seq", ignorenulls=True).over(w10).alias("last_known_event_seq"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("month", "start_date_minus_40d").alias("activity_month"))
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("event_seq_2", 0.9).alias("event_seq_2_p90"),
        F.count("*").alias("event_count_2"),
        F.percentile_approx("seq_no_bucket", 0.5).alias("seq_no_bucket_p50"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
