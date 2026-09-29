from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_unit_price_tier = Window.partitionBy("unit_price_tier")
w_latest = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price_tier_min_unit_price").desc(), F.col("id").desc())

valid_moves = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("unit_price") <= 367.7))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.when(F.col("unit_price") >= 329.7, "A").when(F.col("unit_price") >= 136.0, "B").otherwise("C").alias("unit_price_tier"),
    )
)

metrics = (
    valid_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "unit_price_tier",
        F.sum("qty").over(w_unit_price_tier).alias("unit_price_tier_sum_qty"),
        F.min("unit_price").over(w_unit_price_tier).alias("unit_price_tier_min_unit_price"),
    )
)

without_matches = (
    metrics.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") > 91) & (F.col("st.operator") != "op_eve"), "left_semi")
)

deduped = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_children_moves = (
    deduped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.hour(F.col("st.moved_at")).between(7, 11)), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.moved_at",
        "s.unit_price",
        "s.unit_price_tier",
        "s.unit_price_tier_sum_qty",
        "s.unit_price_tier_min_unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

joined_warehouse = (
    with_children_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "left")
    .filter(F.col("w.opened_on") >= F.lit("2024-04-13").cast("date"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.moved_at",
        "s.unit_price",
        "s.unit_price_tier",
        "s.unit_price_tier_sum_qty",
        "s.unit_price_tier_min_unit_price",
        "s.move_id",
        "s.move_qty",
        "s.move_operator",
        "s.move_moved_at",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        "st.move_type",
        F.col("st.operator").alias("move_operator_2"),
    )
)

result = (
    joined_warehouse
    .select(
        "id",
        "moved_at",
        "unit_price",
        "unit_price_tier",
        "unit_price_tier_sum_qty",
        "unit_price_tier_min_unit_price",
        "move_id",
        "move_qty",
        "move_operator",
        "warehouse_opened_on",
        "move_type",
        "move_operator_2",
        F.quarter(F.col("move_moved_at")).alias("move_moved_at_quarter"),
        F.when(F.col("unit_price_tier_sum_qty") >= 3000, "large").when(F.col("unit_price_tier_sum_qty") >= 2251, "medium").otherwise("small").alias("unit_price_tier_sum_qty_tier"),
    )
    .orderBy(F.col("unit_price_tier_min_unit_price").asc(), F.col("moved_at").desc())
)
