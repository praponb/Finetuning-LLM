from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_operator_by_unit_pric = Window.partitionBy("sku_id", "operator").orderBy(F.col("unit_price").desc(), "id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_operator_by_qty = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id")
w_operator_by_qty_full = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("unit_price").desc(), F.col("id").desc())
w_sku_id_seq_no_by_unit_price = Window.partitionBy("sku_id", "seq_no").orderBy(F.col("unit_price").desc())
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc(), "id")
w_seq_no = Window.partitionBy("seq_no")

scoped_moves = (
    df_stock_moves
    .filter(~F.col("move_type").isin("INBOUND", "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

windowed = (
    scoped_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        F.row_number().over(w_sku_id_operator_by_unit_pric).alias("seq_no"),
        F.min("unit_price").over(w_sku_id_operator).alias("sku_id_operator_min_unit_price"),
        F.ntile(10).over(w_operator_by_qty).alias("qty_decile"),
        F.first("qty").over(w_operator_by_qty_full).alias("first_qty"),
    )
)

latest = (
    windowed
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    latest
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        "seq_no",
        "qty_decile",
        "first_qty",
        F.round(F.percent_rank().over(w_sku_id_seq_no_by_unit_price), 3).alias("unit_price_percent_rank"),
        F.lead("first_qty").over(w_warehouse_id_by_moved_at).alias("next_first_qty"),
        F.sum("first_qty").over(w_seq_no).alias("seq_no_sum_first_qty"),
        F.row_number().over(w_warehouse_id_by_moved_at).alias("moved_at_seq"),
        F.count(F.lit(1)).over(w_seq_no).alias("rows_in_seq_no"),
    )
)

labeled = (
    ranked
    .withColumn("greatest_seq_no_sum_first_qty_seq_no", F.greatest(F.col("seq_no_sum_first_qty"), F.col("seq_no")))
)

with_children = (
    labeled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.seq_no",
        "s.qty_decile",
        "s.first_qty",
        "s.unit_price_percent_rank",
        "s.seq_no_sum_first_qty",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

with_parent_sku = (
    with_children.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.qty_decile",
        "s.first_qty",
        "s.unit_price_percent_rank",
        "s.seq_no_sum_first_qty",
        "s.move_id",
        "s.move_qty",
        "s.move_operator",
        "s.move_sku_id",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.category").alias("sku_category"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    with_parent_sku
    .groupBy("sku_id")
    .pivot("move_operator", ["op_ann", "op_bee", "op_chan"])
    .agg(F.max("qty_decile").alias("max_v"), F.count("qty_decile").alias("n"))
    .orderBy("sku_id")
)
