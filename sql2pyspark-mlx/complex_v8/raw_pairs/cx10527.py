from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium = Window.orderBy("premium", "id")
w_all_by_premium_full = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_month = Window.partitionBy(F.month("start_date"))
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

without_matches = (
    base_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount").between(7204.1, 33690.0)) & (F.col("c.adjuster_id").isNotNull()), "left_semi")
)

metrics = (
    without_matches
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_premium), 3).alias("premium_percent_rank"),
        F.row_number().over(w_all_by_premium).alias("rn"),
        F.first("product_len").over(w_all_by_premium_full).alias("first_product_len"),
        F.round(F.col("product_len") / F.nullif(F.sum("product_len").over(w_start_date_month), F.lit(0)), 4).alias("product_len_share_of_start_date_month"),
        F.first("premium", ignorenulls=True).over(w_all_by_premium_running).alias("first_known_premium"),
        F.ntile(10).over(w_all_by_premium).alias("premium_decile"),
    )
)

product_stats = (
    metrics
    .groupBy("product")
    .agg(F.max("rn").alias("max_rn"))
)

vs_group = (
    metrics.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.rn") == F.col("g.max_rn"))
    .select(
        "s.*",
        "g.max_rn",
        F.round(F.col("s.rn") - F.col("g.max_rn"), 2).alias("rn_minus_max"),
    )
)

result = (
    vs_group
    .cube("premium_decile", "first_product_len")
    .agg(
        F.grouping("premium_decile").alias("is_total_premium_decile"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
    )
    .orderBy(
        F.col("premium_decile").asc_nulls_last(),
        F.col("first_product_len").asc_nulls_last(),
    )
)
