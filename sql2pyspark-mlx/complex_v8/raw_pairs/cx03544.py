from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_moved_at_date_month_wa = Window.partitionBy(F.date_trunc("month", "first_moved_at_date"), "warehouse_id").orderBy("total_qty", "warehouse_id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_dedup = Window.partitionBy("move_operator").orderBy(F.col("total_qty_share_of_warehouse_id").desc(), F.col("warehouse_id").desc(), F.col("move_id").desc())

cleaned_moves = (
    df_stock_moves
    .filter((F.col("unit_price") > 200.7) | (F.col("move_type") == "ADJUST"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_ann", "op_eve"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group"),
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

tagged = (
    cleaned_moves
    .select(
        "*",
        F.date_trunc("quarter", F.col("moved_at_date")).alias("moved_at_date_quarter"),
        F.abs(F.col("qty") - 230).alias("qty_distance"),
    )
)

enriched_warehouse = (
    tagged.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.operator_group",
        "s.operator_prefix",
        "s.moved_at_date",
        "s.moved_at_date_quarter",
        "s.qty_distance",
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

rollup_base = (
    enriched_warehouse
    .groupBy("warehouse_id")
    .agg(
        F.round(F.sum("unit_price") / F.nullif(F.sum("qty_distance"), F.lit(0)), 3).alias("unit_price_to_qty_distance_ratio"),
        F.min("moved_at_date").alias("first_moved_at_date"),
        F.array_sort(F.collect_set("warehouse_wh_code")).alias("warehouse_wh_code_list"),
        F.sum("qty").alias("total_qty"),
        F.sum("unit_price").alias("sum_unit_price_check"),
    )
    .filter(F.col("sum_unit_price_check") > 86.3)
    .drop("sum_unit_price_check")
)

sequenced = (
    rollup_base
    .withColumn("total_qty_change", F.col("total_qty") - F.lag("total_qty").over(w_first_moved_at_date_month_wa))
    .withColumn("total_qty_share_of_warehouse_id", F.round(F.col("total_qty") / F.nullif(F.sum("total_qty").over(w_warehouse_id), F.lit(0)), 4))
)

exploded_detail = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id") >= 10), "left")
    .select(
        "s.warehouse_id",
        "s.unit_price_to_qty_distance_ratio",
        "s.total_qty_change",
        "s.total_qty_share_of_warehouse_id",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.operator").alias("move_operator"),
    )
)

latest_per_key = (
    exploded_detail
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "warehouse_id",
        "unit_price_to_qty_distance_ratio",
        "total_qty_change",
        "total_qty_share_of_warehouse_id",
        "move_id",
        "move_type",
        "move_unit_price",
        "move_sku_id",
        "move_operator",
    )
)
