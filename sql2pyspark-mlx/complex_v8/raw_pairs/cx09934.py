from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy("reported_at", "id")
w_policy_product_policy_holder = Window.partitionBy("policy_product", "policy_holder_id").orderBy(F.col("reported_at").desc_nulls_first(), "id")
w_adjuster_id_by_claim_date = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").asc_nulls_last(), "id")
w_policy_product_policy_holder_2 = Window.partitionBy("policy_product", "policy_holder_id").orderBy(F.col("reported_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_claim_date_ti = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").asc_nulls_last())
w_adjuster_id_by_claim_date_ru = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_reported_at_run = Window.partitionBy("policy_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_reported_at_ful = Window.partitionBy("policy_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_product_policy_holder_3 = Window.partitionBy("policy_product", "policy_holder_id").orderBy(F.col("reported_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_dense_rank_polic = Window.partitionBy("reported_at_dense_rank", "policy_holder_id")
w_all_by_next_claim_date_ties = Window.orderBy("next_claim_date")
w_policy_holder_id_by_next_cla = Window.partitionBy("policy_holder_id").orderBy(F.col("next_claim_date").desc(), "id")
w_all_by_next_claim_date_full = Window.orderBy("next_claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_next_claim_date = Window.orderBy("next_claim_date", "id")

base_claims = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-10-15").cast("date")) & (F.col("policy_id") >= 17))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

enriched_policy = (
    base_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.reported_at",
        "s.claim_type_clean",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
    )
)

ranked = (
    enriched_policy
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "reported_at",
        "claim_type_clean",
        "policy_holder_id",
        F.lead("claim_date").over(w_policy_id_by_reported_at).alias("next_claim_date"),
        F.ntile(5).over(w_policy_product_policy_holder).alias("reported_at_quintile"),
        F.ntile(10).over(w_adjuster_id_by_claim_date).alias("claim_date_decile"),
        F.first("reported_at", ignorenulls=True).over(w_policy_product_policy_holder_2).alias("first_known_reported_at"),
        F.dense_rank().over(w_policy_product_policy_holder).alias("reported_at_dense_rank"),
        F.round(F.cume_dist().over(w_adjuster_id_by_claim_date_ti), 3).alias("claim_date_cume_dist"),
        F.round(F.percent_rank().over(w_policy_product_policy_holder), 3).alias("reported_at_percent_rank"),
        F.last("reported_at", ignorenulls=True).over(w_adjuster_id_by_claim_date_ru).alias("last_known_reported_at"),
        F.first("reported_at", ignorenulls=True).over(w_policy_id_by_reported_at_run).alias("first_known_reported_at_2"),
        F.last("reported_at").over(w_policy_id_by_reported_at_ful).alias("last_reported_at"),
        F.first("claim_date").over(w_policy_product_policy_holder_3).alias("first_claim_date"),
        F.round(F.cume_dist().over(w_policy_product_policy_holder), 3).alias("reported_at_cume_dist"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_policy_id_by_reported_at)).alias("days_since_prev_claim_date"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.max("reported_at_dense_rank").over(w_reported_at_dense_rank_polic).alias("reported_at_dense_rank_policy_holder_id_max_repo"),
        F.round(F.cume_dist().over(w_all_by_next_claim_date_ties), 3).alias("next_claim_date_cume_dist"),
        F.round((F.unix_timestamp("last_known_reported_at") - F.unix_timestamp(F.lag("last_known_reported_at").over(w_policy_holder_id_by_next_cla))) / 60, 1).alias("minutes_since_prev_last_known_reported_at"),
        F.first("claim_date_cume_dist").over(w_all_by_next_claim_date_full).alias("first_claim_date_cume_dist"),
        F.max("claim_date_cume_dist").over(w_reported_at_dense_rank_polic).alias("reported_at_dense_rank_policy_holder_id_max_clai"),
        F.round((F.unix_timestamp("last_reported_at") - F.unix_timestamp(F.lag("last_reported_at").over(w_all_by_next_claim_date))) / 60, 1).alias("minutes_since_prev_last_reported_at"),
        F.size(F.collect_set("adjuster_id").over(w_reported_at_dense_rank_polic)).alias("distinct_adjuster_id_in_reported_at_dense_rank_p"),
    )
)

result = (
    ranked_2
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "reported_at",
        "claim_type_clean",
        "next_claim_date",
        "reported_at_quintile",
        "claim_date_decile",
        "reported_at_dense_rank",
        "claim_date_cume_dist",
        "reported_at_percent_rank",
        "last_known_reported_at",
        "first_known_reported_at_2",
        "last_reported_at",
        "reported_at_cume_dist",
        "days_since_prev_claim_date",
        "reported_at_dense_rank_policy_holder_id_max_repo",
        "next_claim_date_cume_dist",
        "minutes_since_prev_last_known_reported_at",
        "first_claim_date_cume_dist",
        "reported_at_dense_rank_policy_holder_id_max_clai",
        "minutes_since_prev_last_reported_at",
        "distinct_adjuster_id_in_reported_at_dense_rank_p",
    )
    .orderBy(F.col("reported_at_cume_dist").desc_nulls_first())
)
