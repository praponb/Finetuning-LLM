from pyspark.sql import functions as F

prepared = (
    df_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.when(F.col("city").isin("Bangkok", "Singapore"), "group_a").when(F.col("city") == "Kuala Lumpur", "group_b").otherwise("other").alias("city_group"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.operator") == "op_dew") & (F.col("st.sku_id").isNotNull()), "left_anti")
)

stats = (
    with_activity
    .groupBy(F.date_trunc("month", "opened_on").alias("opened_on_month"), "city")
    .agg(
        F.sum(F.when(F.col("wh_code").isNotNull(), F.col("capacity"))).alias("capacity_wh_code_is_not_null"),
        F.count(F.when(F.upper(F.trim(F.col("wh_code"))).like("K%"), 1)).alias("n_upper_trim_wh_code_like_k"),
        F.count(F.when(F.lower(F.col("wh_code")).like("%l1%"), 1)).alias("n_lower_wh_code_like_l1"),
        F.array_sort(F.collect_set("city")).alias("city_list"),
        F.round(F.sum("capacity_filled") / F.nullif(F.sum("days_since_opened_on"), F.lit(0)), 3).alias("capacity_filled_to_days_since_opened_on_ratio"),
        F.count("*").alias("row_count"),
    )
)

derived = (
    stats
    .select(
        "*",
        F.year(F.col("opened_on_month")).alias("opened_on_month_year"),
        F.when(F.col("n_lower_wh_code_like_l1") >= 8, "large").when(F.col("n_lower_wh_code_like_l1") >= 4, "medium").otherwise("small").alias("n_lower_wh_code_like_l1_tier"),
    )
)

exploded = (
    derived
    .select(
        "opened_on_month",
        "city",
        "capacity_wh_code_is_not_null",
        "n_upper_trim_wh_code_like_k",
        "n_lower_wh_code_like_l1",
        "capacity_filled_to_days_since_opened_on_ratio",
        "row_count",
        "opened_on_month_year",
        "n_lower_wh_code_like_l1_tier",
        F.explode("city_list").alias("city_list_item"),
    )
)

result = (
    exploded
    .select(
        "opened_on_month",
        "city",
        F.col("n_upper_trim_wh_code_like_k").alias("m_n_upper_trim_wh_code_like_k"),
        F.col("row_count").alias("m_row_count"),
        F.col("opened_on_month_year").alias("m_opened_on_month_year"),
        F.col("n_lower_wh_code_like_l1").alias("m_n_lower_wh_code_like_l1"),
    )
    .unpivot(
        ids=["opened_on_month", "city"],
        values=["m_n_upper_trim_wh_code_like_k", "m_row_count", "m_opened_on_month_year", "m_n_lower_wh_code_like_l1"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
