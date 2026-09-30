from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_products_distinct_p = Window.partitionBy("distinct_products", "distinct_products_bucket")

scoped = (
    df_policies
    .filter(F.col("product").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

rollup_base = (
    scoped
    .filter(F.col("holder_id") >= 16)
    .groupBy("holder_id", "status")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.sum(F.when(~F.col("status").isin("LAPSED", "CANCELLED"), F.col("premium"))).alias("premium_status_not_in_lapsed_cancell"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.countDistinct("product").alias("distinct_products"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("cnt") >= 2)
)

labeled = (
    rollup_base
    .filter(F.size(F.col("status_list")) >= 1)
    .select(
        "holder_id",
        "status",
        "premium_status_not_in_lapsed_cancell",
        "product_list",
        "distinct_products",
        "status_list",
        (F.floor(F.col("distinct_products") / 2) * 2).alias("distinct_products_bucket"),
        F.when(F.col("status").isin("ACTIVE", "LAPSED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

with_parent_holder = (
    labeled.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.segment") == "RETAIL")
    .select(
        "s.*",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

per_label = (
    with_parent_holder
    .select(
        "holder_id",
        "status",
        "premium_status_not_in_lapsed_cancell",
        "product_list",
        "distinct_products",
        "distinct_products_bucket",
        "status_group",
        "status_prefix",
        "holder_risk_score",
        "holder_joined_on",
        "holder_segment",
        F.explode_outer("status_list").alias("status_list_item"),
    )
)

with_windows = (
    per_label
    .select(
        "*",
        F.count(F.lit(1)).over(w_distinct_products_distinct_p).alias("rows_in_distinct_products_distinct_products_buck"),
        F.min("distinct_products_bucket").over(w_distinct_products_distinct_p).alias("distinct_products_distinct_products_bucket_min_d"),
        F.round(F.col("distinct_products") - F.avg("distinct_products").over(w_distinct_products_distinct_p), 2).alias("distinct_products_vs_distinct_products_distinct_"),
        F.round(F.col("distinct_products_bucket") / F.nullif(F.sum("distinct_products_bucket").over(w_distinct_products_distinct_p), F.lit(0)), 4).alias("distinct_products_bucket_share_of_distinct_produ"),
        F.round(F.col("distinct_products_bucket") - F.avg("distinct_products_bucket").over(w_distinct_products_distinct_p), 2).alias("distinct_products_bucket_vs_distinct_products_di"),
        F.sum("distinct_products_bucket").over(w_distinct_products_distinct_p).alias("distinct_products_distinct_products_bucket_sum_d"),
    )
)

result = (
    with_windows
    .select(
        "holder_id",
        "premium_status_not_in_lapsed_cancell",
        "distinct_products",
        "status_prefix",
        "holder_risk_score",
        "holder_joined_on",
        "holder_segment",
        "rows_in_distinct_products_distinct_products_buck",
        "distinct_products_distinct_products_bucket_min_d",
        "distinct_products_bucket_share_of_distinct_produ",
        "distinct_products_bucket_vs_distinct_products_di",
        "distinct_products_distinct_products_bucket_sum_d",
        F.coalesce(F.col("holder_segment"), F.lit("UNKNOWN")).alias("holder_segment_or_unknown"),
    )
    .orderBy(
        F.col("holder_segment_or_unknown").asc_nulls_last(),
        F.col("distinct_products_distinct_products_bucket_sum_d").desc(),
    )
)
