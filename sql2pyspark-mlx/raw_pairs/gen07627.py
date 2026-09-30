from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("start_date"), "status").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("WEEK", "start_date"), "status").orderBy(F.col("monthly_fee_whole").desc())
w3 = Window.partitionBy(F.year("start_date"), "status").orderBy("monthly_fee")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-08-03"))) & F.col("customer_id").isNotNull())
    .withColumn("plan_status_key", F.concat_ws("/", "plan", "status"))
    .withColumn("monthly_fee_whole", F.ceil("monthly_fee"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "plan_status_key",
        "monthly_fee_whole",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_6"),
        F.percent_rank().over(w2).alias("monthly_fee_whole_percent_rank"),
        F.dense_rank().over(w3).alias("monthly_fee_dense_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("monthly_fee") > 230)
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "plan_status_key",
        "monthly_fee_whole",
        "monthly_fee_avg_6",
        "monthly_fee_dense_rank",
        F.when(F.col("plan").isin(["enterprise", "premium"]), "Priority").otherwise("Other").alias("plan_group"),
    )
)

cleaned_subscriptions = (
    base
    .filter(F.col("plan").isin(["enterprise", "basic"]))
    .select(
        "id",
        "plan",
        "monthly_fee",
        "monthly_fee_whole",
        "monthly_fee_avg_6",
        "monthly_fee_dense_rank",
        "plan_group",
        F.round("monthly_fee_avg_6", 0).alias("monthly_fee_avg_6_rounded"),
        (F.col("monthly_fee_whole") + F.col("monthly_fee")).alias("monthly_fee_whole_plus_monthly_fee"),
    )
)

result = (
    cleaned_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "monthly_fee_avg_6",
        "monthly_fee_dense_rank",
        "monthly_fee_avg_6_rounded",
    )
)
