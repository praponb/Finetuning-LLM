from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("least_monthly_fee_plan_length", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "customer_id", "monthly_fee", F.length("plan").alias("plan_length"))
)

valid_subscriptions = (
    valid
    .filter(F.col("plan_length") != 24)
    .select(
        "*",
        F.least("monthly_fee", "plan_length").alias("least_monthly_fee_plan_length"),
        F.round((F.col("plan_length") * 1.1), 2).alias("plan_length_scaled"),
    )
)

metrics = (
    valid_subscriptions
    .filter(F.col("plan_length_scaled") > 54)
    .select(
        "id",
        "customer_id",
        "plan_length_scaled",
        F.sum("plan_length").over(w).alias("running_sum_plan_length"),
        F.avg("plan_length").over(w).alias("running_avg_plan_length"),
        F.max("plan_length").over(w).alias("running_max_plan_length"),
        F.avg("plan_length").over(w2).alias("plan_length_avg_6"),
    )
)

filtered = (
    metrics
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan_length_scaled",
        "running_sum_plan_length",
        "plan_length_avg_6",
        F.least("plan_length_scaled", "running_max_plan_length").alias("least_plan_length_scaled_running_max_plan_length"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.credit_limit"))
)

matched_keys = (
    enriched
    .select("id", "customer_id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("status") == "EXPIRED")
        .select("id", "customer_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "plan_length_scaled", "running_sum_plan_length", "credit_limit")
    .orderBy(F.col("running_sum_plan_length").asc_nulls_first())
)
