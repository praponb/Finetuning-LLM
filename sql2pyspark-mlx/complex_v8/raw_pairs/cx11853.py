from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_warehouse_capacity = Window.orderBy("warehouse_capacity", "id")
w_all_by_warehouse_capacity_pr = Window.orderBy("warehouse_capacity", "id").rowsBetween(-1, -1)
w_all_by_warehouse_capacity_ti = Window.orderBy("warehouse_capacity")

cleaned = (
    df_stock_moves
    .filter((F.col("qty") > 102) & (F.month(F.col("moved_at")).isin(1, 2)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(": ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id), 2).alias("unit_price_vs_warehouse_id_avg"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
    )
)

tagged = (
    ranked
    .filter(F.col("qty") <= 271)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("operator_move_type_label")), " ", "_")).alias("operator_move_type_label_slug"),
        F.when(F.col("unit_price") >= 330.0, "large").when(F.col("unit_price") >= 215.4, "medium").otherwise("small").alias("unit_price_tier"),
        F.least(F.col("least_unit_price_qty"), F.col("rows_in_warehouse_id")).alias("least_least_unit_price_qty_rows_in_warehouse_id"),
    )
)

combined_details = (
    tagged.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.operator_move_type_label",
        "s.least_unit_price_qty",
        "s.unit_price_bucket",
        "s.rows_in_warehouse_id",
        "s.unit_price_vs_warehouse_id_avg",
        "s.distinct_sku_id_in_warehouse_id",
        "s.operator_move_type_label_slug",
        "s.least_least_unit_price_qty_rows_in_warehouse_id",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        "sk.sku_code",
    )
)

qualified = (
    combined_details.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") >= 78) & (F.col("st.operator") == "op_ann"), "left_semi")
)

ranked_2 = (
    qualified
    .select(
        "*",
        F.row_number().over(w_all_by_warehouse_capacity).alias("seq_no"),
        F.sum("least_unit_price_qty").over(w_all_by_warehouse_capacity_pr).alias("least_unit_price_qty_sum_1_prior"),
        F.rank().over(w_all_by_warehouse_capacity_ti).alias("warehouse_capacity_rank"),
    )
)

screened = (
    ranked_2.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.month(F.col("st.moved_at")).isin(2, 3)) & (F.col("st.qty") >= 168), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "rows_in_warehouse_id",
        "seq_no",
        "distinct_sku_id_in_warehouse_id",
        "qty",
    )
    .unpivot(
        ids=["id"],
        values=["rows_in_warehouse_id", "seq_no", "distinct_sku_id_in_warehouse_id", "qty"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
