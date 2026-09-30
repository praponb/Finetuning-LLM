from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment", "plan").orderBy(F.col("credit_limit").desc())
w4 = Window.partitionBy("customer_id", "status").orderBy(F.col("end_date").desc(), "id")
w_recent = Window.partitionBy("customer_id").orderBy("running_credit_limit", "id")
w6 = Window.partitionBy("segment").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id").orderBy("last_known_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("segment").orderBy("rank_in_group", "id", "subscription_id").rowsBetween(-6, -1)
w9 = Window.orderBy("running_credit_limit", "id", "subscription_id").rowsBetween(-6, Window.currentRow)
w10 = Window.orderBy("rank_in_group", "id", "subscription_id").rowsBetween(-2, -1)

base = (
    df_subscriptions
    .filter(F.col("start_date") > F.to_date(F.lit("2023-08-02")))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("monthly_fee") >= 268, "High").when(F.col("monthly_fee") >= 35, "Medium").otherwise("Low").alias("monthly_fee_tier"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.name"), F.col("c.credit_limit"), F.col("c.segment"))
)

metrics = (
    joined
    .withColumn("running_credit_limit", F.sum("credit_limit").over(w))
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w2))
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w3))
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

metrics_subscriptions = (
    ranked
    .withColumn("prev_running_credit_limit", F.lag("running_credit_limit", 1).over(w_recent))
    .withColumn("last_known_monthly_fee_2", F.last("monthly_fee", ignorenulls=True).over(w6))
    .withColumn("last_known_running_credit_limit", F.last("running_credit_limit", ignorenulls=True).over(w7))
)

matched = (
    metrics_subscriptions
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.start_date") >= F.col("sub.start_date")) & (F.col("a.start_date") <= F.date_add(F.col("sub.end_date"), 7)), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    matched
    .select(
        "*",
        F.avg("last_known_monthly_fee").over(w8).alias("last_known_monthly_fee_avg_6_prior"),
        F.avg("prev_running_credit_limit").over(w9).alias("prev_running_credit_limit_avg_6"),
        F.avg("prev_running_credit_limit").over(w10).alias("prev_running_credit_limit_avg_2_prior"),
    )
)
