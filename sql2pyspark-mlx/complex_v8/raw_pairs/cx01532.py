from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_start_date_year = Window.partitionBy("product", F.year("start_date"))
w_product_by_premium = Window.partitionBy("product").orderBy(F.col("premium").desc(), "id")
w_product_by_premium_ties = Window.partitionBy("product").orderBy(F.col("premium").desc())
w_product_premium_decile = Window.partitionBy("product", "premium_decile")
w_product_by_rn = Window.partitionBy("product").orderBy(F.col("rn").asc_nulls_last(), "id")
w_rn_by_premium_share_of_produ = Window.partitionBy("rn").orderBy(F.col("premium_share_of_product_start_date_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_rn_full = Window.partitionBy("product").orderBy(F.col("rn").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_policies
    .filter(F.col("product") == "TRAVEL")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

windowed = (
    cleaned
    .select(
        "id",
        "product",
        "premium",
        "status",
        F.count(F.lit(1)).over(w_product_start_date_year).alias("rows_in_product_start_date_year"),
        F.row_number().over(w_product_by_premium).alias("rn"),
        F.ntile(10).over(w_product_by_premium).alias("premium_decile"),
        F.sum("premium").over(w_product_start_date_year).alias("product_start_date_year_sum_premium"),
        F.round(F.cume_dist().over(w_product_by_premium_ties), 3).alias("premium_cume_dist"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_start_date_year), F.lit(0)), 4).alias("premium_share_of_product_start_date_year"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

with_stats = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

screened = (
    with_stats
    .crossJoin(with_stats.agg(F.max("premium_cume_dist").alias("overall_max_premium_cume_dist")))
    .filter(F.col("premium_cume_dist") < F.col("overall_max_premium_cume_dist"))
    .drop("overall_max_premium_cume_dist")
)

metrics = (
    screened
    .select(
        "id",
        "product",
        "premium",
        "rows_in_product_start_date_year",
        "rn",
        "product_start_date_year_sum_premium",
        "premium_cume_dist",
        "claim_count",
        F.count(F.lit(1)).over(w_product_premium_decile).alias("rows_in_product_premium_decile"),
        F.round(F.col("claim_count") / F.nullif(F.sum("claim_count").over(w_product_premium_decile), F.lit(0)), 4).alias("claim_count_share_of_product_premium_decile"),
        F.ntile(4).over(w_product_by_rn).alias("rn_quartile"),
        F.first("premium_cume_dist", ignorenulls=True).over(w_rn_by_premium_share_of_produ).alias("first_known_premium_cume_dist"),
        F.row_number().over(w_product_by_rn).alias("seq_no"),
        F.first("premium_cume_dist").over(w_product_by_rn_full).alias("first_premium_cume_dist"),
    )
)

totals = (
    metrics
    .groupBy("product")
    .agg(
        F.count("*").alias("n_rows"),
        F.percentile_approx("first_known_premium_cume_dist", 0.5).alias("median_first_known_premium_cume_dist"),
        F.sum("rows_in_product_premium_decile").alias("total_rows_in_product_premium_decile"),
        F.round(F.sum("first_known_premium_cume_dist") / F.nullif(F.sum("seq_no"), F.lit(0)), 3).alias("first_known_premium_cume_dist_to_seq_no_ratio"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    totals
    .filter(F.col("total_rows_in_product_premium_decile") > 19)
    .select(
        "product",
        "n_rows",
        "median_first_known_premium_cume_dist",
        "total_rows_in_product_premium_decile",
        "first_known_premium_cume_dist_to_seq_no_ratio",
    )
    .orderBy(
        F.col("total_rows_in_product_premium_decile").desc(),
        F.col("median_first_known_premium_cume_dist").desc(),
    )
)
