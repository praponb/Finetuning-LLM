from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")
w_recent = Window.partitionBy("customer_id").orderBy("customer_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("next_monthly_fee").desc_nulls_first())
w_grp = Window.partitionBy("customer_id").orderBy("signup_date", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("next_monthly_fee", "id")

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= "2023-03-01") & F.col("customer_id").isNotNull())
    .withColumn("plan_status_key", F.concat_ws("-", "plan", "status"))
)

with_dims = (
    valid
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.start_date",
        "src.end_date",
        "src.monthly_fee",
        F.col("c.city"),
        F.col("c.credit_limit"),
    )
)

metrics = (
    with_dims
    .select(
        "*",
        F.lead("credit_limit").over(w).alias("next_credit_limit"),
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.row_number().over(w).alias("seq_no"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.city").isNotNull())
    .select(
        "e.id",
        "e.customer_id",
        "e.start_date",
        "e.next_credit_limit",
        "e.next_monthly_fee",
        "e.seq_no",
        F.col("b.city").alias("customer_city"),
        F.col("b.signup_date"),
        F.col("b.age"),
    )
)

enriched = (
    joined
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.next_monthly_fee",
        "src.age",
        F.col("b.signup_date").alias("customer_signup_date"),
        F.col("b.age").alias("customer_age"),
        F.col("b.email"),
    )
)

metrics_subscriptions = (
    enriched
    .withColumn("running_avg_next_monthly_fee", F.avg("next_monthly_fee").over(w_recent))
    .withColumn("running_count_next_monthly_fee", F.count("next_monthly_fee").over(w_recent))
    .withColumn("running_min_next_monthly_fee", F.min("next_monthly_fee").over(w_recent))
    .withColumn("next_monthly_fee_dense_rank", F.dense_rank().over(w3))
)

joined_subscriptions = (
    metrics_subscriptions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.next_monthly_fee",
        "s.age",
        "s.customer_signup_date",
        "s.running_count_next_monthly_fee",
        "s.running_min_next_monthly_fee",
        "s.next_monthly_fee_dense_rank",
        F.col("c.signup_date"),
    )
)

result = (
    joined_subscriptions
    .select(
        "id",
        "customer_id",
        "customer_signup_date",
        "signup_date",
        F.min("running_count_next_monthly_fee").over(w_grp).alias("last7_min_running_count_next_monthly_fee"),
        F.max("running_count_next_monthly_fee").over(w_grp).alias("last7_max_running_count_next_monthly_fee"),
        F.avg("running_count_next_monthly_fee").over(w_grp).alias("last7_avg_running_count_next_monthly_fee"),
        F.row_number().over(w5).alias("row_seq"),
    )
)
