from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_end_date = Window.partitionBy("product").orderBy("end_date", "id")
w_product_by_end_date_ties = Window.partitionBy("product").orderBy("end_date")

cleaned = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-05-30").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        "s.product_clean",
        F.col("p.segment").alias("holder_segment"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "holder_id",
        "end_date",
        "product_clean",
        "holder_segment",
        F.row_number().over(w_product_by_end_date).alias("rn"),
        F.round(F.percent_rank().over(w_product_by_end_date_ties), 3).alias("end_date_percent_rank"),
    )
)

summary = (
    with_windows
    .groupBy(F.date_trunc("month", "end_date").alias("end_date_month"), "holder_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("end_date_percent_rank").alias("total_end_date_percent_rank"),
        F.countDistinct("holder_segment").alias("distinct_holder_segments"),
        F.array_sort(F.collect_set("holder_segment")).alias("holder_segment_list"),
        F.round(F.avg("end_date_percent_rank"), 2).alias("avg_end_date_percent_rank"),
        F.round(F.sum("rn") / F.nullif(F.sum("end_date_percent_rank"), F.lit(0)), 3).alias("rn_to_end_date_percent_rank_ratio"),
    )
)

combined_details = (
    summary.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.end_date_month",
        "s.holder_id",
        "s.n_rows",
        "s.total_end_date_percent_rank",
        "s.distinct_holder_segments",
        "s.holder_segment_list",
        "s.avg_end_date_percent_rank",
        "s.rn_to_end_date_percent_rank_ratio",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

with_children = (
    combined_details.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.end_date_month",
        "s.holder_id",
        "s.total_end_date_percent_rank",
        "s.distinct_holder_segments",
        "s.holder_segment_list",
        "s.avg_end_date_percent_rank",
        "s.rn_to_end_date_percent_rank_ratio",
        "s.holder_joined_on",
        F.col("p.id").alias("policy_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
    )
)

combined_details_2 = (
    with_children.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.end_date_month",
        "s.holder_id",
        "s.total_end_date_percent_rank",
        "s.distinct_holder_segments",
        "s.avg_end_date_percent_rank",
        "s.rn_to_end_date_percent_rank_ratio",
        "s.policy_id",
        "s.policy_premium",
        "s.policy_start_date",
        "s.policy_status",
        "s.policy_product",
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    combined_details_2
    .select(
        "end_date_month",
        "holder_id",
        "total_end_date_percent_rank",
        "distinct_holder_segments",
        "avg_end_date_percent_rank",
        "rn_to_end_date_percent_rank_ratio",
        "policy_id",
        "policy_premium",
        "policy_start_date",
        "policy_status",
        "policy_product",
        "holder_region",
    )
    .orderBy(
        F.col("distinct_holder_segments").asc(),
        F.col("policy_start_date").desc(),
    )
)
