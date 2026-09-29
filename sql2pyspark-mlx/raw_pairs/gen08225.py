from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("customer_id").orderBy("age", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy(F.col("credit_limit_int").desc(), "id")
w4 = Window.orderBy("credit_limit", "id")
w5 = Window.partitionBy("plan").orderBy("credit_limit")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "plan")
)

base = (
    latest
    .filter(F.col("customer_id") == 23)
    .withColumn("clean_plan", F.trim("plan"))
)

with_dims = (
    base
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.segment") == "Consumer")
    .select(
        "e.id",
        "e.customer_id",
        "e.plan",
        "e.clean_plan",
        F.col("d.credit_limit"),
        F.col("d.age"),
        F.col("d.email"),
    )
)

base_subscriptions = (
    with_dims
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.col("credit_limit").cast("int").alias("credit_limit_int"))
)

metrics = (
    base_subscriptions
    .select(
        "id",
        "plan",
        "credit_limit",
        F.max("credit_limit_int").over(w2).alias("last4_max_credit_limit_int"),
        F.min("credit_limit_int").over(w2).alias("last4_min_credit_limit_int"),
        F.sum("credit_limit_int").over(w2).alias("last4_sum_credit_limit_int"),
        F.ntile(10).over(w3).alias("credit_limit_int_bucket"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "credit_limit",
        "last4_min_credit_limit_int",
        F.row_number().over(w4).alias("event_seq"),
        F.cume_dist().over(w5).alias("credit_limit_cume_dist"),
    )
)

result = (
    scored
    .select(
        "id",
        F.col("credit_limit").alias("CREDIT_LIMIT"),
        F.col("credit_limit_cume_dist").alias("CREDIT_LIMIT_CUME_DIST_VAL"),
        F.col("last4_min_credit_limit_int").alias("LAST4_MIN_CREDIT_LIMIT_INT_VAL"),
    )
    .unpivot(
        ids=["id"],
        values=["CREDIT_LIMIT", "CREDIT_LIMIT_CUME_DIST_VAL", "LAST4_MIN_CREDIT_LIMIT_INT_VAL"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
