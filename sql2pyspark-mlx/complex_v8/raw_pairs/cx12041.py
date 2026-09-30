from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_claim_date_quarter_year = Window.partitionBy(F.year("last_claim_date_quarter")).orderBy("total_min_claim_approved_amount")
w_all_by_max_claimed_amount_ti = Window.orderBy(F.col("max_claimed_amount").desc())
w_last_claim_date_quarter_year_2 = Window.partitionBy(F.year("last_claim_date_quarter")).orderBy("total_min_claim_approved_amount", "claim_date_quarter_day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_max_claimed_amount_pr = Window.orderBy(F.col("max_claimed_amount").desc(), "claim_date_quarter_day").rowsBetween(-6, -1)
w_last_claim_date_quarter_year_3 = Window.partitionBy(F.year("last_claim_date_quarter")).orderBy("total_min_claim_approved_amount", "claim_date_quarter_day")
w_all_by_max_claimed_amount_fu = Window.orderBy(F.col("max_claimed_amount").desc(), "claim_date_quarter_day").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_claims = (
    df_claims
    .filter(F.month(F.col("reported_at")).isin(1, 2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
    )
)

summarized_rows = (
    scoped_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.min_claim_approved_amount",
    )
)

stats = (
    summarized_rows
    .groupBy(F.to_date("claim_date_quarter").alias("claim_date_quarter_day"))
    .agg(
        F.max("claimed_amount").alias("max_claimed_amount"),
        F.round(F.sum("claimed_amount_per_approved_amount") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount_to_claimed_am"),
        F.sum(F.when(F.col("claim_type") != "theft", F.col("claimed_amount_per_approved_amount")).otherwise(0)).alias("claimed_amount_per_approved_amount_claim_type_th"),
        F.max("claim_date_quarter").alias("last_claim_date_quarter"),
        F.round(F.sum("min_claim_approved_amount"), 2).alias("total_min_claim_approved_amount"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.avg("claim_count").alias("avg_claim_count_check"),
    )
    .filter(F.col("avg_claim_count_check") > 1)
    .drop("avg_claim_count_check")
)

metrics = (
    stats
    .select(
        "claim_date_quarter_day",
        "max_claimed_amount",
        "claimed_amount_per_approved_amount_to_claimed_am",
        "claimed_amount_per_approved_amount_claim_type_th",
        "total_min_claim_approved_amount",
        "claim_type_list",
        F.round(F.cume_dist().over(w_last_claim_date_quarter_year), 3).alias("total_min_claim_approved_amount_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_max_claimed_amount_ti), 3).alias("max_claimed_amount_percent_rank"),
        F.max("max_claimed_amount").over(w_last_claim_date_quarter_year_2).alias("max_max_claimed_amount_so_far"),
        F.round(F.avg("max_claimed_amount").over(w_all_by_max_claimed_amount_pr), 2).alias("max_claimed_amount_avg_6_prior"),
        (F.col("max_claimed_amount") - F.lag("max_claimed_amount").over(w_last_claim_date_quarter_year_3)).alias("max_claimed_amount_change"),
        F.nth_value("claim_date_quarter_day", 2).over(w_all_by_max_claimed_amount_fu).alias("second_claim_date_quarter_day"),
    )
)

annotated = (
    metrics
    .select(
        "claim_date_quarter_day",
        "max_claimed_amount",
        "claimed_amount_per_approved_amount_to_claimed_am",
        "claimed_amount_per_approved_amount_claim_type_th",
        "total_min_claim_approved_amount",
        "total_min_claim_approved_amount_cume_dist",
        "max_claimed_amount_percent_rank",
        "max_max_claimed_amount_so_far",
        "max_claimed_amount_avg_6_prior",
        "max_claimed_amount_change",
        "second_claim_date_quarter_day",
        F.add_months(F.col("claim_date_quarter_day"), 3).alias("claim_date_quarter_day_plus_3m"),
        F.date_format(F.col("claim_date_quarter_day"), "yyyy-MM-dd").alias("claim_date_quarter_day_label"),
        F.round(F.col("max_claimed_amount") * 1.1, 2).alias("max_claimed_amount_adj"),
    )
)

labeled = (
    annotated
    .select(
        "claim_date_quarter_day",
        "max_claimed_amount",
        "claimed_amount_per_approved_amount_to_claimed_am",
        "claimed_amount_per_approved_amount_claim_type_th",
        "total_min_claim_approved_amount",
        "total_min_claim_approved_amount_cume_dist",
        "second_claim_date_quarter_day",
        "claim_date_quarter_day_plus_3m",
        "claim_date_quarter_day_label",
        "max_claimed_amount_adj",
        F.when(F.col("max_claimed_amount") >= 37525.0, "large").when(F.col("max_claimed_amount") >= 12575.0, "medium").otherwise("small").alias("max_claimed_amount_tier"),
        F.date_trunc("week", F.col("claim_date_quarter_day")).alias("claim_date_quarter_day_week"),
    )
)

result = (
    labeled
    .select(
        "claim_date_quarter_day",
        "max_claimed_amount",
        "claimed_amount_per_approved_amount_to_claimed_am",
        "total_min_claim_approved_amount",
        "total_min_claim_approved_amount_cume_dist",
        "second_claim_date_quarter_day",
        "claim_date_quarter_day_plus_3m",
        "claim_date_quarter_day_label",
        "max_claimed_amount_tier",
        "claim_date_quarter_day_week",
        F.when(F.col("max_claimed_amount") >= 42515.0, "HIGH").when(F.col("max_claimed_amount") >= 22555.0, "MEDIUM").otherwise("LOW").alias("max_claimed_amount_tier_2"),
        F.when(F.col("max_claimed_amount") >= 37525.0, "HIGH").when(F.col("max_claimed_amount") >= 17565.0, "MEDIUM").otherwise("LOW").alias("max_claimed_amount_tier_3"),
    )
    .orderBy(
        "claim_date_quarter_day_week",
        F.col("second_claim_date_quarter_day").asc(),
    )
)
