from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())
w3 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w4 = Window.partitionBy("status", "plan")
w5 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")

base = (
    df_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-10-23"))) & F.col("customer_id").isNotNull())
    .select("*", F.concat_ws("-", "plan", "status").alias("plan_status_key"))
)

metrics = (
    base
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "plan_status_key",
        F.sum("monthly_fee").over(w).alias("cumulative_monthly_fee"),
        F.percent_rank().over(w2).alias("monthly_fee_percent_rank"),
        F.dense_rank().over(w3).alias("monthly_fee_dense_rank"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w4)), 4).alias("monthly_fee_share"),
        F.lag("monthly_fee", 1, 0).over(w5).alias("prev_monthly_fee"),
    )
)

joined = (
    metrics
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.signup_date"), F.col("c.segment"))
)

with_window = (
    joined
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.start_date") >= F.col("sub.start_date")) & (F.col("a.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .select(
        "id",
        "subscription_id",
        "customer_id",
        F.col("monthly_fee").alias("MONTHLY_FEE_VAL"),
        F.col("monthly_fee_percent_rank").alias("MONTHLY_FEE_PERCENT_RANK_VAL"),
        F.col("prev_monthly_fee").alias("PREV_MONTHLY_FEE"),
    )
    .unpivot(
        ids=["id", "subscription_id", "customer_id"],
        values=["MONTHLY_FEE_VAL", "MONTHLY_FEE_PERCENT_RANK_VAL", "PREV_MONTHLY_FEE"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
