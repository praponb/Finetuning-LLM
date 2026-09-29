from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id", "product").orderBy("days_end_date_to_start_date", "id", "claim_id")

scoped_policys = (
    df_policies
    .filter((F.col("status").isNotNull()) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
    )
)

exploded_detail_claims = (
    scoped_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") != "PAID"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.days_end_date_to_start_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

derived = (
    exploded_detail_claims
    .select(
        "*",
        F.date_format(F.col("end_date"), "yyyy-MM-dd").alias("end_date_label"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

labeled = (
    derived
    .withColumn("greatest_premium_claim_claimed_amount", F.greatest(F.col("premium"), F.col("claim_claimed_amount")))
)

deduped = (
    labeled
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

totals = (
    deduped
    .groupBy("claim_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.percentile_approx("days_end_date_to_start_date", 0.5).alias("median_days_end_date_to_start_date"),
        F.round(F.sum("claim_approved_amount") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("claim_approved_amount_to_premium_ratio"),
        F.countDistinct("claim_status").alias("distinct_claim_status"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
        F.avg("greatest_premium_claim_claimed_amount").alias("avg_greatest_premium_claim_claimed_amount_check"),
    )
    .filter(F.col("avg_greatest_premium_claim_claimed_amount_check") > 20060.0)
    .drop("avg_greatest_premium_claim_claimed_amount_check")
)

tagged = (
    totals
    .filter(F.col("cnt") > 2)
    .select(
        "claim_id",
        "cnt",
        "claim_approved_amount_to_premium_ratio",
        "distinct_claim_status",
        "stddev_premium",
        (F.floor(F.col("stddev_premium") / 1000) * 1000).alias("stddev_premium_bucket"),
        F.when(F.col("distinct_claim_status") >= 7, "A").when(F.col("distinct_claim_status") >= 3, "B").otherwise("C").alias("distinct_claim_status_tier"),
    )
)

result = (
    tagged
    .select(
        "claim_id",
        "cnt",
        "claim_approved_amount_to_premium_ratio",
        "distinct_claim_status",
        "stddev_premium",
    )
    .orderBy(F.col("claim_approved_amount_to_premium_ratio").desc(), "claim_id")
    .limit(5)
)
