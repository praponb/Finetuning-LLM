from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_claim_adjuster_id_by = Window.partitionBy("product", "claim_adjuster_id").orderBy("premium", "id", "claim_id")
w_claim_adjuster_id = Window.partitionBy("claim_adjuster_id")

base_policys = (
    df_policies
    .filter(F.col("premium") >= 2096.6)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

detail_rows_claims = (
    base_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.status_slug",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

with_trends = (
    detail_rows_claims
    .select(
        "*",
        F.lag("premium").over(w_product_claim_adjuster_id_by).alias("prev_premium"),
        F.size(F.collect_set("product").over(w_claim_adjuster_id)).alias("distinct_product_in_claim_adjuster_id"),
        F.row_number().over(w_product_claim_adjuster_id_by).alias("seq_no"),
        F.lead("premium", 3).over(w_product_claim_adjuster_id_by).alias("next3_premium"),
        F.round(F.avg("premium").over(w_claim_adjuster_id), 2).alias("claim_adjuster_id_avg_premium"),
        F.ntile(10).over(w_product_claim_adjuster_id_by).alias("premium_decile"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.status_slug",
        "s.claim_id",
        "s.claim_type",
        "s.claim_adjuster_id",
        "s.distinct_product_in_claim_adjuster_id",
        "s.seq_no",
        "s.next3_premium",
        "s.premium_decile",
        F.col("p.region").alias("holder_region"),
    )
)

line_level = (
    joined.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.claim_id",
        "s.claim_type",
        "s.claim_adjuster_id",
        "s.seq_no",
        "s.holder_region",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

claim_status_stats = (
    line_level
    .groupBy("claim_status")
    .agg(F.avg("seq_no").alias("avg_seq_no"))
)

outliers = (
    line_level.alias("s")
    .join(claim_status_stats.alias("g"), F.col("g.claim_status") == F.col("s.claim_status"), "inner")
    .filter(F.col("s.seq_no") > F.col("g.avg_seq_no"))
    .select(
        "s.*",
        "g.avg_seq_no",
        F.round(F.col("s.seq_no") - F.col("g.avg_seq_no"), 2).alias("seq_no_minus_avg"),
    )
)

per_group = (
    outliers
    .filter(F.col("seq_no") >= 4)
    .groupBy("claim_id_2")
    .agg(
        F.count(F.when(F.col("holder_region") == "Central", 1)).alias("n_holder_region_central"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
    )
)

result = (
    per_group
    .select(
        "claim_id_2",
        F.array_join(F.array_sort(F.col("claim_type_list")), ",").alias("claim_type_list_text"),
    )
    .orderBy(F.col("claim_id_2").desc())
)
