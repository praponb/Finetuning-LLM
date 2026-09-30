from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_len_moved_at_month = Window.partitionBy("operator_len", F.month("moved_at"))
w_operator_warehouse_city_by_o = Window.partitionBy("operator", "warehouse_city").orderBy("operator_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_warehouse_city_by_o_2 = Window.partitionBy("operator", "warehouse_city").orderBy("operator_len")
w_warehouse_id_by_unit_price_r = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_unit_price_p = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-2, -1)
w_dedup = Window.partitionBy("sku_id", "operator_group").orderBy(F.col("unit_price_bucket").desc(), F.col("id").desc())

recent = (
    df_stock_moves
    .filter(F.col("move_type").isin("ADJUST", "TRANSFER", "OUTBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_dew", "op_ann"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.min("qty").alias("min_move_qty"))
)

summarized_rows = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.min_move_qty")
)

joined = (
    summarized_rows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.operator_group",
        "s.unit_price_bucket",
        "s.operator_len",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

with_trends = (
    joined
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator_group",
        "unit_price_bucket",
        "warehouse_capacity",
        F.round(F.avg("warehouse_capacity").over(w_operator_len_moved_at_month), 2).alias("operator_len_moved_at_month_avg_warehouse_capaci"),
        F.first("moved_at", ignorenulls=True).over(w_operator_warehouse_city_by_o).alias("first_known_moved_at"),
        F.round(F.cume_dist().over(w_operator_warehouse_city_by_o_2), 3).alias("operator_len_cume_dist"),
        F.last("warehouse_opened_on", ignorenulls=True).over(w_warehouse_id_by_unit_price_r).alias("last_known_warehouse_opened_on"),
        F.sum("operator_len").over(w_warehouse_id_by_unit_price_p).alias("operator_len_sum_2_prior"),
    )
)

latest = (
    with_trends
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

combined_details_warehouse = (
    latest.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.opened_on").isNotNull())
    .select("s.*", F.col("w.opened_on").alias("warehouse_opened_on"))
)

exploded_detail_moves = (
    combined_details_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator_group",
        "s.warehouse_capacity",
        "s.operator_len_sum_2_prior",
        "s.warehouse_opened_on",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        "st.move_type",
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    exploded_detail_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "warehouse_capacity",
        "operator_len_sum_2_prior",
        "warehouse_opened_on",
        "move_id",
        "move_moved_at",
        "move_type",
        "move_qty",
        "move_operator",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("move_moved_at")).alias("days_since_move_moved_at"),
        (F.floor(F.col("operator_len_sum_2_prior") / 5) * 5).alias("operator_len_sum_2_prior_bucket"),
    )
    .orderBy(F.col("move_id").desc(), "id")
    .limit(5)
)
