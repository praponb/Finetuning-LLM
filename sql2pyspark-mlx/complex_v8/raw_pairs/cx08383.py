from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_id = Window.partitionBy("product", "holder_id")
w_all_by_premium_adj = Window.orderBy("premium_adj", "id")
w_all_by_premium_adj_full = Window.orderBy("premium_adj", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_prior_3 = Window.orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_all_by_premium = Window.orderBy(F.col("premium").desc_nulls_first(), "id")

base_policys = (
    df_policies
    .filter((F.col("product") == "HOME") & (F.col("status") == "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
    )
)

combined_details = (
    base_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.lower(F.col("p.holder_name")).like("%on%"))
    .select(
        "s.*",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.round(F.col("premium_adj") / F.nullif(F.sum("premium_adj").over(w_product_holder_id), F.lit(0)), 4).alias("premium_adj_share_of_product_holder_id"),
        F.lag("premium_adj").over(w_all_by_premium_adj).alias("prev_premium_adj"),
        F.round(F.cume_dist().over(w_all_by_premium_adj), 3).alias("premium_adj_cume_dist"),
        F.size(F.collect_set("holder_segment").over(w_product_holder_id)).alias("distinct_holder_segment_in_product_holder_id"),
        F.last("premium_adj").over(w_all_by_premium_adj_full).alias("last_premium_adj"),
        F.nth_value("holder_risk_score", 2).over(w_all_by_premium_adj_full).alias("second_holder_risk_score"),
        F.round(F.avg("holder_risk_score").over(w_product_holder_id), 2).alias("product_holder_id_avg_holder_risk_score"),
        F.round(F.avg("holder_risk_score").over(w_all_by_premium_prior_3), 2).alias("holder_risk_score_avg_3_prior"),
        F.round(F.percent_rank().over(w_all_by_premium), 3).alias("premium_percent_rank"),
        F.count(F.lit(1)).over(w_product_holder_id).alias("rows_in_product_holder_id"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_holder_id), F.lit(0)), 4).alias("premium_share_of_product_holder_id"),
    )
)

tagged = (
    with_windows
    .filter(F.col("last_premium_adj").between(1660.0, 6040.0))
    .select(
        "*",
        F.round(F.col("holder_risk_score") / F.nullif(F.col("premium_adj_cume_dist"), F.lit(0)), 3).alias("holder_risk_score_per_premium_adj_cume_dist"),
        F.coalesce(F.col("premium_percent_rank"), F.lit(0.0)).alias("premium_percent_rank_filled"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

summary = (
    tagged
    .groupBy(
        "holder_segment",
        "distinct_holder_segment_in_product_holder_id",
        "holder_id",
    )
    .agg(
        F.round(F.avg("premium_adj_share_of_product_holder_id"), 2).alias("avg_premium_adj_share_of_product_holder_id"),
        F.sum(F.when(F.col("product").isNotNull(), F.col("rows_in_product_holder_id"))).alias("rows_in_product_holder_id_product_is_not_null"),
        F.sum("product_holder_id_avg_holder_risk_score").alias("sum_product_holder_id_avg_holder_risk_score_chec"),
    )
    .filter(F.col("sum_product_holder_id_avg_holder_risk_score_chec") > 0.2)
    .drop("sum_product_holder_id_avg_holder_risk_score_chec")
)

result = (
    summary
    .filter(F.col("holder_segment").isNotNull())
    .select(
        "holder_segment",
        "distinct_holder_segment_in_product_holder_id",
        "holder_id",
        "avg_premium_adj_share_of_product_holder_id",
    )
    .orderBy("avg_premium_adj_share_of_product_holder_id")
)
