from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("start_date", "id")
w3 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("status").orderBy(F.col("monthly_fee_avg_5_prior").desc())
w5 = Window.partitionBy("status")
w6 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy(F.col("next_monthly_fee").desc(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-07-06").cast("date"))
    .withColumn("start_date_month_end", F.last_day("start_date"))
)

enriched = (
    cleaned
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.lead("monthly_fee", 1, 0).over(w2).alias("next_monthly_fee"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_5_prior"),
    )
)

scored = (
    enriched
    .withColumn("monthly_fee_avg_5_prior_rank", F.rank().over(w4))
    .withColumn("monthly_fee_share_2", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w5)), 4))
    .withColumn("last_known_monthly_fee_share", F.last("monthly_fee_share", ignorenulls=True).over(w6))
    .withColumn("first_known_next_monthly_fee", F.first("next_monthly_fee", ignorenulls=True).over(w7))
    .withColumn("next_monthly_fee_bucket", F.ntile(10).over(w8))
)

kept_keys = (
    scored
    .select("id", "status")
    .intersectAll(
        df_subscriptions
        .filter(F.month("start_date").isin([9, 10, 11]))
        .select("id", "status")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("monthly_fee") < 170)
    .select("*", F.date_format("end_date", "yyyy-MM").alias("end_date_label"))
)

filtered = (
    valid
    .filter(F.col("monthly_fee_share_2").between(0.36, 0.44))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        "monthly_fee_share",
        "next_monthly_fee",
        "monthly_fee_avg_5_prior_rank",
        "monthly_fee_share_2",
        "last_known_monthly_fee_share",
        "next_monthly_fee_bucket",
        "end_date_label",
        F.ceil("first_known_next_monthly_fee").alias("first_known_next_monthly_fee_whole"),
    )
)

result = (
    filtered
    .select(
        "id",
        "status",
        "next_monthly_fee",
        "monthly_fee_share_2",
        "last_known_monthly_fee_share",
        "first_known_next_monthly_fee_whole",
        F.when(F.col("status").rlike("^(EXPIRED|PAUSED)$"), "PRIORITY").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("status").asc_nulls_last())
)
