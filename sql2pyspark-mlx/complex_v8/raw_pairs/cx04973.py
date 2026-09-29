from pyspark.sql import functions as F

cleaned = (
    df_warehouses
    .filter(F.col("opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-12-17").cast("date")))
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        F.year(F.col("opened_on")).alias("opened_on_year"),
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
    )
)

labeled = (
    cleaned
    .filter(~F.col("city").isin("Singapore", "Bangkok"))
    .select(
        "*",
        F.concat(F.col("city"), F.lit("#"), F.col("id").cast("string")).alias("city_ref"),
    )
)

derived = (
    labeled
    .filter(F.year(F.col("opened_on")) == 2025)
    .select(
        "id",
        "wh_code",
        "opened_on_year",
        "capacity_bucket",
        "city_ref",
        F.date_trunc("week", F.col("opened_on")).alias("opened_on_week"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.min("qty").alias("min_move_qty"))
)

summarized_rows = (
    derived.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.min_move_qty")
)

grouped = (
    summarized_rows
    .groupBy(F.year("opened_on_week").alias("opened_on_week_year"), "opened_on_year")
    .agg(
        F.array_sort(F.collect_set("wh_code")).alias("wh_code_list"),
        F.round(F.avg("capacity_bucket"), 2).alias("avg_capacity_bucket"),
        F.sum("opened_on_year").alias("total_opened_on_year"),
        F.sum("min_move_qty").alias("total_min_move_qty"),
        F.round(100 * F.count(F.when(F.lower(F.col("wh_code")).like("%nx%"), 1)) / F.count("*"), 1).alias("pct_lower_wh_code_like_nx"),
        F.count(F.when(F.upper(F.trim(F.col("wh_code"))).like("C%"), 1)).alias("n_upper_trim_wh_code_like_c"),
    )
)

result = (
    grouped
    .select(
        "opened_on_week_year",
        "opened_on_year",
        "total_opened_on_year",
        "pct_lower_wh_code_like_nx",
        "n_upper_trim_wh_code_like_c",
        F.coalesce(F.col("total_min_move_qty"), F.lit(0)).alias("total_min_move_qty_filled"),
        F.when(F.col("opened_on_year") >= 2025, "HIGH").when(F.col("opened_on_year") >= 2024, "MEDIUM").otherwise("LOW").alias("opened_on_year_tier"),
    )
    .orderBy("pct_lower_wh_code_like_nx", "opened_on_year_tier")
)
