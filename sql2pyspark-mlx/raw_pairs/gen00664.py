from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city", "plan")
w3 = Window.partitionBy("customer_id", "plan")
w4 = Window.orderBy(F.col("customer_id_min_monthly_fee").asc_nulls_last())
w5 = Window.orderBy("monthly_fee_share")
w6 = Window.partitionBy("segment")

matched_keys = (
    df_subscriptions
    .select("customer_id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("customer_id")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.city").isin(["Bangkok", "London"]))
    .select("a.*", F.col("b.city"), F.col("b.signup_date"), F.col("b.segment"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.email"),
        F.col("d.segment").alias("customer_segment"),
        F.col("d.name"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "plan",
        "status",
        "city",
        "segment",
        "customer_segment",
        "name",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share_2"),
        F.min("monthly_fee").over(w3).alias("customer_id_min_monthly_fee"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "city",
        "customer_segment",
        "monthly_fee_share",
        "monthly_fee_share_2",
        "customer_id_min_monthly_fee",
        F.rank().over(w4).alias("customer_id_min_monthly_fee_rank"),
        F.cume_dist().over(w5).alias("monthly_fee_share_cume_dist"),
        F.avg("monthly_fee_share").over(w6).alias("segment_avg_monthly_fee_share"),
    )
)

base = (
    with_windows
    .filter(F.col("city").isNotNull() & F.col("customer_id_min_monthly_fee").isNotNull())
    .withColumn("customer_id_min_monthly_fee_rank_tier", F.when(F.col("customer_id_min_monthly_fee_rank") >= 7, "High").when(F.col("customer_id_min_monthly_fee_rank") >= 3, "Medium").otherwise("Low"))
    .withColumn("customer_segment_clean", F.coalesce("customer_segment", F.lit("NONE")))
)

cleaned = (
    base
    .filter(~F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .withColumn("monthly_fee_share_cume_dist_plus_segment_avg_monthly_fee_share", (F.col("monthly_fee_share_cume_dist") + F.col("segment_avg_monthly_fee_share")))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("customer_id_min_monthly_fee_rank") >= 7, "HIGH").otherwise("LOW").alias("customer_id_min_monthly_fee_rank_tier"),
    )
    .agg(
        F.avg("monthly_fee_share").alias("avg_monthly_fee_share"),
        F.sum(F.when(F.col("customer_segment") == "Corporate", F.col("segment_avg_monthly_fee_share")).otherwise(0)).alias("corporate_segment_avg_monthly_fee_share"),
        F.round(F.sum("customer_id_min_monthly_fee_rank") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("customer_id_min_monthly_fee_rank_per_chiang_mai"),
        F.percentile_approx("customer_id_min_monthly_fee_rank", 0.5).alias("customer_id_min_monthly_fee_rank_p50"),
        F.round(F.sum("monthly_fee_share") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("monthly_fee_share_per_new_york"),
    )
    .orderBy("customer_id_min_monthly_fee_rank_tier")
)
