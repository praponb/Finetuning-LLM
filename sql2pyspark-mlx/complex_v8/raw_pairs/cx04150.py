from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator = Window.partitionBy("operator")
w_row_count = Window.partitionBy("row_count")
w_warehouse_city_row_count_by = Window.partitionBy("warehouse_city", "row_count").orderBy(F.col("distinct_ids").desc_nulls_first(), "warehouse_city", "warehouse_opened_on_month", "operator")

cleaned = (
    df_stock_moves
    .filter((F.col("unit_price") > 119.5) & (F.col("operator").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

without_matches = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id").isNotNull()), "left_anti")
)

combined_details_warehouse = (
    without_matches.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_clean",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

ranked = (
    combined_details_warehouse
    .select(
        "*",
        F.sum("unit_price").over(w_all_by_unit_price_running).alias("running_unit_price"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
    )
)

summary = (
    ranked
    .groupBy(
        "warehouse_city",
        F.date_trunc("month", "warehouse_opened_on").alias("warehouse_opened_on_month"),
        "operator",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.upper(F.trim(F.col("warehouse_wh_code"))).like("C%"), 1).otherwise(0)).alias("n_upper_trim_warehouse_wh_code"),
        F.min("rows_in_operator").alias("min_rows_in_operator"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

ranked_2 = (
    summary
    .select(
        "warehouse_city",
        "warehouse_opened_on_month",
        "operator",
        "row_count",
        "n_upper_trim_warehouse_wh_code",
        "min_rows_in_operator",
        "distinct_ids",
        F.round(F.col("min_rows_in_operator") - F.avg("min_rows_in_operator").over(w_row_count), 2).alias("min_rows_in_operator_vs_row_count_avg"),
        F.round(F.col("distinct_ids") - F.avg("distinct_ids").over(w_row_count), 2).alias("distinct_ids_vs_row_count_avg"),
        (F.col("row_count") - F.lag("row_count").over(w_warehouse_city_row_count_by)).alias("row_count_change"),
    )
)

result = (
    ranked_2
    .select(
        "warehouse_city",
        "row_count",
        "n_upper_trim_warehouse_wh_code",
        "min_rows_in_operator",
        "distinct_ids",
        "min_rows_in_operator_vs_row_count_avg",
        "distinct_ids_vs_row_count_avg",
        "row_count_change",
    )
    .orderBy(F.col("row_count_change").desc())
)
