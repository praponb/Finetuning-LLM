from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_count_by_min_claim_app = Window.partitionBy("claim_count").orderBy("min_claim_approved_amount", "id")
w_product = Window.partitionBy("product")
w_holder_id_by_end_date_full = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_holder_id_end_date_tercile_b = Window.partitionBy("holder_id", "end_date_tercile").orderBy("claim_count_share_of_product")
w_claim_count_end_date_year_by = Window.partitionBy("claim_count", F.year("end_date")).orderBy("claim_count_share_of_product")
w_end_date_tercile_by_premium = Window.partitionBy("end_date_tercile").orderBy(F.col("premium_distance_per_premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_tercile_by_premium_2 = Window.partitionBy("end_date_tercile").orderBy(F.col("premium_distance_per_premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_policys = (
    df_policies
    .filter(~F.col("status").isin("CANCELLED", "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("approved_amount").alias("min_claim_approved_amount"),
        F.count("*").alias("claim_count"),
    )
)

with_counts = (
    prepared_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.min_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

metrics = (
    with_counts
    .select(
        "*",
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_claim_count_by_min_claim_app)).alias("days_since_prev_end_date"),
        F.round(F.col("claim_count") / F.nullif(F.sum("claim_count").over(w_product), F.lit(0)), 4).alias("claim_count_share_of_product"),
        F.first("premium_distance").over(w_holder_id_by_end_date_full).alias("first_premium_distance"),
        F.ntile(3).over(w_holder_id_by_end_date).alias("end_date_tercile"),
        F.round(F.col("start_date_quarter") / F.nullif(F.sum("start_date_quarter").over(w_product), F.lit(0)), 4).alias("start_date_quarter_share_of_product"),
    )
)

labeled = (
    metrics
    .filter(F.col("first_premium_distance") <= 2750.0)
    .select(
        "*",
        F.round(F.col("first_premium_distance") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("first_premium_distance_per_premium"),
        F.lower(F.trim(F.col("status_clean"))).alias("status_clean_clean"),
        F.round(F.col("premium_distance") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("premium_distance_per_premium"),
    )
)

ranked = (
    labeled
    .select(
        "*",
        F.rank().over(w_holder_id_end_date_tercile_b).alias("claim_count_share_of_product_rank"),
        F.round(F.cume_dist().over(w_claim_count_end_date_year_by), 3).alias("claim_count_share_of_product_cume_dist"),
        F.first("premium_distance", ignorenulls=True).over(w_end_date_tercile_by_premium).alias("first_known_premium_distance"),
        F.first("claim_count_share_of_product").over(w_end_date_tercile_by_premium_2).alias("first_claim_count_share_of_product"),
    )
)

labeled_2 = (
    ranked
    .withColumn("days_since_start_date", F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")))
)

result = (
    labeled_2
    .filter(F.col("first_known_premium_distance") > 1500.0)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "start_date_quarter",
        "premium_distance",
        "min_claim_approved_amount",
        "claim_count",
        "claim_count_share_of_product",
        "first_premium_distance",
        "end_date_tercile",
        "start_date_quarter_share_of_product",
        "first_premium_distance_per_premium",
        "status_clean_clean",
        "premium_distance_per_premium",
        "claim_count_share_of_product_rank",
        "claim_count_share_of_product_cume_dist",
        "first_known_premium_distance",
        "first_claim_count_share_of_product",
        "days_since_start_date",
        F.date_format(F.col("end_date"), "yyyy-MM-dd").alias("end_date_label"),
        F.when(F.col("claim_count") >= 6, "HIGH").when(F.col("claim_count") >= 4, "MEDIUM").otherwise("LOW").alias("claim_count_tier"),
    )
    .orderBy(
        F.col("premium_distance").desc(),
        F.col("min_claim_approved_amount").desc_nulls_first(),
    )
)
