from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt = Window.partitionBy("cnt")
w_all_by_cnt = Window.orderBy("cnt", "policy_status")
w_n_holder_region_north_by_cnt = Window.partitionBy("n_holder_region_north").orderBy("cnt")
w_n_holder_region_north_by_cnt_2 = Window.partitionBy("n_holder_region_north").orderBy("cnt", "policy_status")
w_n_holder_region_north_by_cnt_3 = Window.partitionBy("n_holder_region_north").orderBy("cnt", "policy_status").rowsBetween(-2, Window.currentRow)
w_policy_status_cnt_quintile = Window.partitionBy("policy_status", "cnt_quintile")
w_cnt_seq_by_n_holder_region_n = Window.partitionBy("cnt_seq").orderBy(F.col("n_holder_region_north_vs_cnt_avg").desc(), "policy_status")

cleaned = (
    df_claims
    .filter(~F.col("claim_type").isin("fire", "flood"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        "s.status_slug",
        F.col("p.status").alias("policy_status"),
        F.col("po.region").alias("holder_region"),
    )
)

summary = (
    enriched
    .groupBy("policy_status")
    .agg(
        F.array_sort(F.collect_set("policy_status")).alias("policy_status_list"),
        F.sum(F.when(F.col("holder_region") != "North", 1).otherwise(0)).alias("n_holder_region_north"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

sequenced = (
    summary
    .select(
        "*",
        F.round(F.col("n_holder_region_north") - F.avg("n_holder_region_north").over(w_cnt), 2).alias("n_holder_region_north_vs_cnt_avg"),
        F.row_number().over(w_all_by_cnt).alias("cnt_seq"),
        F.ntile(5).over(w_all_by_cnt).alias("cnt_quintile"),
        F.round(F.cume_dist().over(w_n_holder_region_north_by_cnt), 3).alias("cnt_cume_dist"),
        F.ntile(4).over(w_n_holder_region_north_by_cnt_2).alias("cnt_quartile"),
        F.max("cnt").over(w_n_holder_region_north_by_cnt_3).alias("cnt_max_3"),
    )
)

windowed = (
    sequenced
    .select(
        "policy_status",
        "n_holder_region_north",
        "cnt_seq",
        "cnt_cume_dist",
        "cnt_max_3",
        F.min("cnt_max_3").over(w_policy_status_cnt_quintile).alias("policy_status_cnt_quintile_min_cnt_max_3"),
        F.lag("cnt_quartile", 3, 0).over(w_cnt_seq_by_n_holder_region_n).alias("prev3_cnt_quartile"),
        F.row_number().over(w_cnt_seq_by_n_holder_region_n).alias("n_holder_region_north_vs_cnt_avg_seq"),
    )
)

first_rows = windowed.filter(F.col("cnt_seq") <= 5)

result = (
    first_rows
    .groupBy("cnt_seq", "prev3_cnt_quartile")
    .pivot("policy_status", ["CANCELLED", "EXPIRED", "LAPSED", "ACTIVE"])
    .agg(F.sum("policy_status_cnt_quintile_min_cnt_max_3").alias("amount"), F.max("policy_status_cnt_quintile_min_cnt_max_3").alias("max_v"))
)
