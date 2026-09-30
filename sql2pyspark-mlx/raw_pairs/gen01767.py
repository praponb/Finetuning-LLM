from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id", "country").orderBy("customer_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country")
w3 = Window.partitionBy(F.month("customer_signup_date"), "plan").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w_ordered = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.year("start_date"), "customer_id").orderBy("running_customer_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("plan", "customer_id").orderBy("first_known_days_start_date_to_end_date", "id")

filtered = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-05-25").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.plan",
        "src.start_date",
        "src.monthly_fee_filled",
        "src.days_start_date_to_end_date",
        F.col("d.age"),
        F.col("d.signup_date"),
        F.col("d.segment"),
    )
)

joined = (
    with_dims
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.email"), F.col("p.city"))
)

joined_subscriptions = (
    joined
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.country") == "US")
    .select(
        "s.*",
        F.col("d.country"),
        F.col("d.signup_date").alias("customer_signup_date"),
        F.col("d.age").alias("customer_age"),
    )
)

metrics = (
    joined_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee_filled",
        "age",
        "email",
        F.first("days_start_date_to_end_date", ignorenulls=True).over(w_recent).alias("first_known_days_start_date_to_end_date"),
        F.max("days_start_date_to_end_date").over(w2).alias("country_max_days_start_date_to_end_date"),
        F.avg("days_start_date_to_end_date").over(w3).alias("days_start_date_to_end_date_avg_3"),
        F.avg("monthly_fee_filled").over(w_ordered).alias("monthly_fee_filled_avg_6"),
        F.sum("customer_age").over(w_hist).alias("running_customer_age"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "customer_id",
        "monthly_fee_filled",
        "age",
        "email",
        "country_max_days_start_date_to_end_date",
        "running_customer_age",
        F.sum("days_start_date_to_end_date_avg_3").over(w6).alias("running_days_start_date_to_end_date_avg_3"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("customer_id").asc_nulls_first())
)
