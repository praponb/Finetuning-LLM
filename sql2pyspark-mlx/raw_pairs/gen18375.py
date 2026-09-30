from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy("start_date", "id")
w2 = Window.partitionBy("status").orderBy("dedup_rank_plus_monthly_fee", "id")
w3 = Window.partitionBy("plan").orderBy(F.col("latest_rank").desc(), "id")

ranked = (
    df_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    ranked
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        (F.col("dedup_rank") + F.col("monthly_fee")).alias("dedup_rank_plus_monthly_fee"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

ranked_subscriptions = (
    latest
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "plan", "monthly_fee", "status", "latest_rank")
)

result = (
    ranked_subscriptions
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 216, "PREMIUM").otherwise("STANDARD"))
    .cube("plan", "monthly_fee_tier")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("monthly_fee_per_cancelled"),
        F.avg(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee"))).alias("cancelled_avg_monthly_fee"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "standard", 1)), F.lit(0)), 2).alias("monthly_fee_per_standard"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.grouping("plan").alias("grouping_id"),
    )
)
