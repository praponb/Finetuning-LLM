from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").asc_nulls_first())
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w4 = Window.orderBy("end_date", "id")
w_hist = Window.partitionBy("customer_id", "status").orderBy("monthly_fee", "id")
w6 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("plan").orderBy(F.col("next_monthly_fee_2").desc(), "id")
w8 = Window.orderBy(F.col("next_monthly_fee").asc_nulls_first())
w9 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_first())
w10 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_bucket_2").desc_nulls_first())
w11 = Window.partitionBy("plan")

ranked = (
    df_subscriptions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    ranked
    .select(
        "*",
        F.ntile(4).over(w2).alias("monthly_fee_bucket"),
        F.lead("monthly_fee", 1).over(w3).alias("next_monthly_fee"),
        F.lead("monthly_fee", 1).over(w4).alias("next_monthly_fee_2"),
        F.ntile(10).over(w_hist).alias("monthly_fee_bucket_2"),
    )
)

scored = (
    enriched
    .withColumn("monthly_fee_avg_5", F.avg("monthly_fee").over(w6))
    .withColumn("next_monthly_fee_2_bucket", F.ntile(4).over(w_recent))
    .withColumn("next_monthly_fee_dense_rank", F.dense_rank().over(w8))
)

matched_keys = (
    scored
    .select("status")
    .intersectAll(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("status")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "*",
        F.percent_rank().over(w9).alias("monthly_fee_percent_rank"),
        F.percent_rank().over(w10).alias("monthly_fee_bucket_2_percent_rank"),
        F.round((F.col("monthly_fee_bucket") / F.sum("monthly_fee_bucket").over(w11)), 4).alias("monthly_fee_bucket_share"),
    )
)

filtered = (
    with_windows
    .filter((F.col("start_date") >= F.lit("2023-04-05").cast("date")) & F.col("customer_id").isNotNull() & F.month("end_date").isin([4, 5, 11]))
    .select(
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_bucket",
        "next_monthly_fee",
        "next_monthly_fee_2",
        "monthly_fee_avg_5",
        "monthly_fee_percent_rank",
        "monthly_fee_bucket_share",
        F.add_months("start_date", 6).alias("start_date_plus_months"),
        F.least("monthly_fee_bucket_2", "monthly_fee").alias("least_monthly_fee_bucket_2_monthly_fee"),
    )
)

grouped = (
    filtered
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("next_monthly_fee")).otherwise(0)).alias("premium_next_monthly_fee"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee_percent_rank")).otherwise(0)).alias("premium_monthly_fee_percent_rank"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
)

result = (
    grouped
    .select(
        "status",
        "premium_next_monthly_fee",
        "cancelled_count",
        F.when(F.col("status").rlike("^(CANCELLED|ACTIVE)$"), "GROUP_B").when(F.col("status").rlike("^(PAUSED|EXPIRED)$"), "CORE").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("cancelled_count").desc_nulls_first())
)
