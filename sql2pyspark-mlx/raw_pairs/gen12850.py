from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").asc_nulls_last())
w2 = Window.partitionBy("customer_id", "status").orderBy(F.col("monthly_fee").desc_nulls_first())
w3 = Window.partitionBy("status").orderBy("start_date", "id")
w4 = Window.partitionBy("plan", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

cleaned = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-08-15").cast("date"))
    .withColumn("status_plan", F.concat("status", F.lit(" - "), "plan"))
    .withColumn("plan_status_key", F.concat_ws(",", "plan", "status"))
)

cleaned_subscriptions = (
    cleaned
    .filter((F.col("end_date") >= F.lit("2023-04-12").cast("date")) & F.col("monthly_fee").between(210, 240))
    .select("*", F.trim("plan").alias("clean_plan"))
)

with_windows = (
    cleaned_subscriptions
    .withColumn("monthly_fee_rank", F.rank().over(w))
    .withColumn("monthly_fee_dense_rank", F.dense_rank().over(w2))
    .withColumn("next_monthly_fee", F.lead("monthly_fee", 1).over(w3))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w4))
)

remaining_keys = (
    with_windows
    .select("status")
    .intersectAll(
        df_subscriptions
        .filter(F.col("plan") == "enterprise")
        .select("status")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status_plan",
        "plan_status_key",
        "clean_plan",
        "monthly_fee_rank",
        F.col("max_subscription_monthly_fee"),
    )
)
