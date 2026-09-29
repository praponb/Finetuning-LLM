from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w_part = Window.partitionBy("customer_country").orderBy("customer_credit_limit", "id")
w3 = Window.orderBy(F.col("customer_credit_limit").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

with_dims = (
    df_sessions
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.signup_date") > "2023-04-21")
    .select(
        "src.id",
        "src.customer_id",
        "src.browser",
        "src.started_at",
        "src.country",
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
)

with_dims_sessions = (
    with_dims
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        F.col("c.name"),
        F.col("c.credit_limit").alias("customer_credit_limit"),
        F.col("c.country").alias("customer_country"),
    )
)

metrics = (
    with_dims_sessions
    .withColumn("customer_id_sum_customer_credit_limit", F.sum("customer_credit_limit").over(w))
    .withColumn("customer_credit_limit_bucket", F.ntile(3).over(w_part))
    .withColumn("customer_credit_limit_bucket_2", F.ntile(4).over(w3))
)

latest_match = (
    metrics
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_credit_limit",
        "s.customer_country",
        "s.customer_id_sum_customer_credit_limit",
        "s.customer_credit_limit_bucket",
        F.col("c.event_time"),
        F.col("c.event_type"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "customer_credit_limit",
        "customer_country",
        "customer_credit_limit_bucket",
        "event_time",
        "event_type",
    )
)
