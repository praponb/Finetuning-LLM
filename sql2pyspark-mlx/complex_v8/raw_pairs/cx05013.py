from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id = Window.partitionBy("status", "holder_id")
w_status_product_by_start_date = Window.partitionBy("status", "product").orderBy("start_date", "id")
w_product_holder_id_by_days_en = Window.partitionBy("product", "holder_id").orderBy("days_end_date_to_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_holder_id_by_days_en_2 = Window.partitionBy("product", "holder_id").orderBy("days_end_date_to_start_date")

recent_policys = (
    df_policies
    .filter(F.col("end_date") < F.lit("2026-10-23").cast("date"))
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

labeled = (
    recent_policys
    .filter(F.col("holder_id") <= 9)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
    )
)

ranked = (
    labeled
    .select(
        "*",
        F.count(F.lit(1)).over(w_status_holder_id).alias("rows_in_status_holder_id"),
        F.ntile(10).over(w_status_product_by_start_date).alias("start_date_decile"),
        F.min("premium").over(w_product_holder_id_by_days_en).alias("min_premium_so_far"),
        F.round(F.cume_dist().over(w_product_holder_id_by_days_en_2), 3).alias("days_end_date_to_start_date_cume_dist"),
        F.round(F.col("days_end_date_to_start_date") - F.avg("days_end_date_to_start_date").over(w_status_holder_id), 2).alias("days_end_date_to_start_date_vs_status_holder_id_"),
        F.row_number().over(w_status_product_by_start_date).alias("start_date_seq"),
    )
)

annotated = (
    ranked
    .select(
        "id",
        "product",
        "premium",
        "status",
        "product_prefix",
        "product_slug",
        "days_end_date_to_start_date",
        "rows_in_status_holder_id",
        "start_date_decile",
        "min_premium_so_far",
        "days_end_date_to_start_date_cume_dist",
        "days_end_date_to_start_date_vs_status_holder_id_",
        "start_date_seq",
        F.round(F.col("premium") * 0.9, 2).alias("premium_adj"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

without_matches = (
    annotated.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") != "REJECTED"), "left_anti")
)

rollup_base = (
    without_matches
    .filter(F.col("premium") > 1042.8)
    .groupBy(
        "start_date_decile",
        F.when(F.col("premium") >= 3817.5, "A").when(F.col("premium") >= 1132.1, "B").otherwise("C").alias("premium_tier"),
        "product",
    )
    .agg(
        F.round(F.avg("rows_in_status_holder_id"), 2).alias("avg_rows_in_status_holder_id"),
        F.count("*").alias("row_count"),
        F.sum("premium").alias("sum_premium_check"),
    )
    .filter(F.col("sum_premium_check") > 2096.6)
    .drop("sum_premium_check")
)

tagged = (
    rollup_base
    .select(
        "*",
        F.coalesce(F.col("start_date_decile"), F.lit(0)).alias("start_date_decile_filled"),
        F.when(F.col("premium_tier").isin("B", "C"), "group_a").when(F.col("premium_tier") == "A", "group_b").otherwise("other").alias("premium_tier_group"),
    )
)

result = (
    tagged
    .groupingSets(
        [["premium_tier_group", "product"], ["premium_tier_group"], ["product"]],
        "premium_tier_group", "product",
    )
    .agg(
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.countDistinct("premium_tier_group").alias("distinct_premium_tier_groups"),
    )
    .select(
        F.coalesce(F.col("premium_tier_group"), F.lit("ALL")).alias("premium_tier_group"),
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "product_list",
        "distinct_premium_tier_groups",
    )
    .orderBy(
        F.col("premium_tier_group").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
    )
)
