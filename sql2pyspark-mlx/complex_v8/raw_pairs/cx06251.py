from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_operator_by_move_qty_2 = Window.partitionBy("move_operator").orderBy(F.col("move_qty_2").desc(), "id", "move_id", "move_id_2", "sku_label").rowsBetween(-3, Window.currentRow)
w_sku_id_move_sku_id_by_sku_un = Window.partitionBy("sku_id", "move_sku_id").orderBy("sku_unit_cost")
w_sku_label_move_operator = Window.partitionBy("sku_label", "move_operator")

base_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 5) & (F.col("sku_id") >= 10))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

joined = (
    base_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

line_level = (
    joined.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.sku_unit_cost",
        "s.sku_category",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

detail_rows = (
    line_level.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.sku_unit_cost",
        "s.move_id",
        "s.move_unit_price",
        "s.move_qty",
        "s.move_operator",
        "s.move_warehouse_id",
        F.col("st.id").alias("move_id_2"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty_2"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.unit_price").alias("move_unit_price_2"),
    )
)

with_details = (
    detail_rows.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.move_sku_id"), "left")
    .select("s.*", F.col("sk.labels").alias("sku_labels"))
)

per_label = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "sku_unit_cost",
        "move_id",
        "move_unit_price",
        "move_qty",
        "move_operator",
        "move_warehouse_id",
        "move_id_2",
        "move_moved_at",
        "move_qty_2",
        "move_sku_id",
        "move_unit_price_2",
        F.explode("sku_labels").alias("sku_label"),
    )
)

windowed = (
    per_label
    .select(
        "*",
        F.round(F.avg("qty").over(w_move_operator_by_move_qty_2), 2).alias("qty_avg_4"),
        F.round(F.percent_rank().over(w_sku_id_move_sku_id_by_sku_un), 3).alias("sku_unit_cost_percent_rank"),
        F.count(F.lit(1)).over(w_sku_label_move_operator).alias("rows_in_sku_label_move_operator"),
        F.size(F.collect_set("move_warehouse_id").over(w_sku_label_move_operator)).alias("distinct_move_warehouse_id_in_sku_label_move_ope"),
    )
)

result = (
    windowed
    .filter(F.col("warehouse_id") >= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "move_id",
        "move_qty",
        "move_operator",
        "move_warehouse_id",
        "move_id_2",
        "move_moved_at",
        "move_unit_price_2",
        "sku_label",
        "qty_avg_4",
        "distinct_move_warehouse_id_in_sku_label_move_ope",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)
