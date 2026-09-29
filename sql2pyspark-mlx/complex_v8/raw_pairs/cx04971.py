from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_distinct_status = Window.orderBy("distinct_status", "product")
w_all_by_distinct_holder_ids = Window.orderBy(F.col("distinct_holder_ids").desc_nulls_first(), "product")
w_product = Window.partitionBy("product")
w_all_by_distinct_status_runni = Window.orderBy("distinct_status", "product").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_distinct_status_full = Window.orderBy("distinct_status", "product").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_status_quintile_dis = Window.partitionBy("distinct_status_quintile", "distinct_holder_ids").orderBy("distinct_status", "product")
w_distinct_status_by_next_dist = Window.partitionBy("distinct_status").orderBy(F.col("next_distinct_status").desc(), "product")
w_distinct_holder_ids_distinct = Window.partitionBy("distinct_holder_ids", "distinct_status_quintile")

prepared = (
    df_policies
    .filter((F.col("product").isNotNull()) & (~F.col("status").isin("LAPSED", "ACTIVE")))
)

with_parent = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

tagged = (
    with_parent
    .filter(F.col("holder_id") <= 11)
    .select(
        "*",
        (F.col("premium") + F.col("holder_risk_score")).alias("premium_plus_holder_risk_score"),
        F.round(F.col("holder_risk_score") * 0.25, 2).alias("holder_risk_score_adj"),
    )
)

summary = (
    tagged
    .groupBy("product")
    .agg(
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.countDistinct("status").alias("distinct_status"),
        F.percentile_approx("premium_plus_holder_risk_score", 0.5).alias("median_premium_plus_holder_risk_score"),
    )
)

metrics = (
    summary
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_distinct_status), 3).alias("distinct_status_percent_rank"),
        F.lead("distinct_status").over(w_all_by_distinct_holder_ids).alias("next_distinct_status"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.max("distinct_holder_ids").over(w_all_by_distinct_status_runni).alias("max_distinct_holder_ids_so_far"),
        F.last("distinct_holder_ids").over(w_all_by_distinct_status_full).alias("last_distinct_holder_ids"),
        F.ntile(5).over(w_all_by_distinct_status).alias("distinct_status_quintile"),
    )
)

derived = (
    metrics
    .select(
        "product",
        "distinct_holder_ids",
        "distinct_status",
        "median_premium_plus_holder_risk_score",
        "distinct_status_percent_rank",
        "next_distinct_status",
        "distinct_status_quintile",
        F.round(F.col("last_distinct_holder_ids") / F.nullif(F.col("rows_in_product"), F.lit(0)), 3).alias("last_distinct_holder_ids_per_rows_in_product"),
        F.coalesce(F.col("distinct_holder_ids"), F.lit(0)).alias("distinct_holder_ids_filled"),
    )
)

metrics_2 = (
    derived
    .select(
        "product",
        "distinct_holder_ids",
        "distinct_status",
        "median_premium_plus_holder_risk_score",
        "next_distinct_status",
        "distinct_status_quintile",
        "distinct_holder_ids_filled",
        F.dense_rank().over(w_distinct_status_quintile_dis).alias("distinct_status_dense_rank"),
        F.ntile(10).over(w_distinct_status_by_next_dist).alias("next_distinct_status_decile"),
        F.round(F.col("distinct_holder_ids_filled") - F.avg("distinct_holder_ids_filled").over(w_distinct_holder_ids_distinct), 2).alias("distinct_holder_ids_filled_vs_distinct_holder_id"),
    )
)

result = (
    metrics_2
    .select(
        "product",
        "distinct_holder_ids",
        "distinct_status",
        "median_premium_plus_holder_risk_score",
        "next_distinct_status",
        "distinct_status_quintile",
        "distinct_holder_ids_filled",
        "distinct_status_dense_rank",
        "next_distinct_status_decile",
        "distinct_holder_ids_filled_vs_distinct_holder_id",
    )
    .orderBy(F.col("distinct_status").desc())
)
