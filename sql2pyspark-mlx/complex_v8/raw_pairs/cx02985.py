from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year_by_premium_b = Window.partitionBy(F.year("start_date")).orderBy("premium_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_year_by_premium_b_2 = Window.partitionBy(F.year("start_date")).orderBy("premium_bucket", "id").rowsBetween(-6, -1)
w_start_date_year_by_premium_b_3 = Window.partitionBy(F.year("start_date")).orderBy("premium_bucket", "id")
w_start_date_month = Window.partitionBy(F.date_trunc("month", "start_date"))
w_latest = Window.partitionBy("holder_id", "holder_segment_2").orderBy(F.col("claim_count_2").desc(), F.col("id").desc())

scoped_policys = (
    df_policies
    .filter((F.col("start_date").isNotNull()) & (F.col("premium").between(2096.6, 4678.8)))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

metrics = (
    scoped_policys
    .select(
        "*",
        F.sum("premium").over(w_start_date_year_by_premium_b).alias("running_premium"),
        F.max("premium").over(w_start_date_year_by_premium_b_2).alias("premium_max_6_prior"),
        F.lag("start_date").over(w_start_date_year_by_premium_b_3).alias("prev_start_date"),
        F.round(F.avg("premium_bucket").over(w_start_date_month), 2).alias("start_date_month_avg_premium_bucket"),
    )
)

joined = (
    metrics.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.premium_bucket",
        "s.running_premium",
        "s.premium_max_6_prior",
        "s.start_date_month_avg_premium_bucket",
        F.col("p.segment").alias("holder_segment"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
        F.count("*").alias("claim_count"),
        F.count("*").alias("claim_count_2"),
    )
)

profiled = (
    joined.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.total_claim_claimed_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

joined_holder = (
    profiled.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium_bucket",
        "s.premium_max_6_prior",
        "s.total_claim_claimed_amount",
        "s.claim_count",
        "s.claim_count_2",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment_2"),
        F.col("p.region").alias("holder_region"),
    )
)

latest_per_key = (
    joined_holder
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "holder_id",
        "premium_max_6_prior",
        "total_claim_claimed_amount",
        "claim_count",
        "claim_count_2",
        "holder_joined_on",
        "holder_segment_2",
        F.concat(F.col("holder_region"), F.lit("#"), F.col("claim_count_2").cast("string")).alias("holder_region_ref"),
    )
)
