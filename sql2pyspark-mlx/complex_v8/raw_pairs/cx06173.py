from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_holder_ids = Window.partitionBy("distinct_holder_ids")
w_distinct_holder_ids_premium = Window.partitionBy("distinct_holder_ids", "premium_tier").orderBy("distinct_holder_ids")
w_all_by_total_premium = Window.orderBy("total_premium", "premium_tier")
w_distinct_holder_ids_distinct = Window.partitionBy("distinct_holder_ids", "distinct_holder_ids_rank")
w_premium_tier_by_distinct_hol = Window.partitionBy("premium_tier").orderBy(F.col("distinct_holder_ids_avg_total_premium").desc(), "premium_tier").rowsBetween(-6, Window.currentRow)
w_all_by_distinct_holder_ids_a = Window.orderBy("distinct_holder_ids_avg_total_premium", "premium_tier").rowsBetween(-6, -1)
w_all_by_distinct_holder_ids_a_2 = Window.orderBy("distinct_holder_ids_avg_total_premium", "premium_tier").rowsBetween(-3, Window.currentRow)
w_premium_tier_or_unknown_tota = Window.partitionBy("premium_tier_or_unknown", "total_premium_quartile").orderBy(F.col("distinct_holder_ids_avg_total_premium").desc(), "premium_tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_total_premium_quartile_premi = Window.partitionBy("total_premium_quartile", "premium_tier")
w_premium_tier_or_unknown_tota_2 = Window.partitionBy("premium_tier_or_unknown", "total_premium_quartile").orderBy(F.col("distinct_holder_ids_avg_total_premium").desc(), "premium_tier").rowsBetween(-2, -1)

scoped_policys = (
    df_policies
    .filter(F.col("status").isin("ACTIVE", "LAPSED", "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.when(F.col("premium") >= 3719.7, "large").when(F.col("premium") >= 2033.8, "medium").otherwise("small").alias("premium_tier"),
    )
)

grouped = (
    scoped_policys
    .filter(F.year(F.col("end_date")) == 2026)
    .groupBy("premium_tier")
    .agg(
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.round(F.sum("premium"), 2).alias("total_premium"),
    )
)

metrics = (
    grouped
    .select(
        "*",
        F.count(F.lit(1)).over(w_distinct_holder_ids).alias("rows_in_distinct_holder_ids"),
        F.rank().over(w_distinct_holder_ids_premium).alias("distinct_holder_ids_rank"),
        F.ntile(4).over(w_all_by_total_premium).alias("total_premium_quartile"),
        F.round(F.avg("total_premium").over(w_distinct_holder_ids), 2).alias("distinct_holder_ids_avg_total_premium"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.size(F.collect_set("premium_tier").over(w_distinct_holder_ids_distinct)).alias("distinct_premium_tier_in_distinct_holder_ids_dis"),
        F.min("total_premium_quartile").over(w_distinct_holder_ids_distinct).alias("distinct_holder_ids_distinct_holder_ids_rank_min"),
        F.round(F.col("total_premium_quartile") - F.avg("total_premium_quartile").over(w_distinct_holder_ids_distinct), 2).alias("total_premium_quartile_vs_distinct_holder_ids_di"),
        F.sum("total_premium").over(w_premium_tier_by_distinct_hol).alias("total_premium_sum_7"),
    )
)

labeled = (
    with_windows
    .select(
        "*",
        F.upper(F.trim(F.col("premium_tier"))).alias("premium_tier_clean"),
        F.concat(F.col("premium_tier"), F.lit("#"), F.col("distinct_holder_ids").cast("string")).alias("premium_tier_ref"),
        F.coalesce(F.col("premium_tier"), F.lit("UNKNOWN")).alias("premium_tier_or_unknown"),
    )
)

ranked = (
    labeled
    .select(
        "*",
        F.max("total_premium").over(w_all_by_distinct_holder_ids_a).alias("total_premium_max_6_prior"),
        F.max("total_premium_quartile").over(w_all_by_distinct_holder_ids_a_2).alias("total_premium_quartile_max_4"),
    )
)

with_windows_2 = (
    ranked
    .select(
        "*",
        F.first("distinct_holder_ids_rank", ignorenulls=True).over(w_premium_tier_or_unknown_tota).alias("first_known_distinct_holder_ids_rank"),
        F.min("total_premium_sum_7").over(w_total_premium_quartile_premi).alias("total_premium_quartile_premium_tier_min_total_pr"),
        F.size(F.collect_set("premium_tier_or_unknown").over(w_total_premium_quartile_premi)).alias("distinct_premium_tier_or_unknown_in_total_premiu"),
        F.round(F.avg("distinct_holder_ids").over(w_premium_tier_or_unknown_tota_2), 2).alias("distinct_holder_ids_avg_2_prior"),
    )
)

result = (
    with_windows_2
    .select(
        "premium_tier",
        "distinct_holder_ids",
        "total_premium",
        "rows_in_distinct_holder_ids",
        "distinct_holder_ids_rank",
        "total_premium_quartile",
        "distinct_holder_ids_avg_total_premium",
        "distinct_holder_ids_distinct_holder_ids_rank_min",
        "total_premium_quartile_vs_distinct_holder_ids_di",
        "premium_tier_clean",
        "premium_tier_or_unknown",
        "total_premium_max_6_prior",
        "total_premium_quartile_max_4",
        "first_known_distinct_holder_ids_rank",
        "total_premium_quartile_premium_tier_min_total_pr",
        "distinct_holder_ids_avg_2_prior",
        F.greatest(F.col("total_premium_sum_7"), F.col("total_premium")).alias("greatest_total_premium_sum_7_total_premium"),
    )
    .orderBy("first_known_distinct_holder_ids_rank", "premium_tier")
    .limit(20)
)
