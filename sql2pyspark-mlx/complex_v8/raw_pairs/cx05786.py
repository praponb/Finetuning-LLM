from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_operator_by_q = Window.partitionBy(F.date_trunc("month", "moved_at"), "operator").orderBy(F.col("qty").desc())
w_warehouse_id = Window.partitionBy("warehouse_id")

recent = (
    df_stock_moves
    .filter((F.col("operator").isNotNull()) & (F.col("move_type") == "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.1, 2).alias("unit_price_adj"),
    )
)

ranked = (
    recent
    .withColumn("qty_cume_dist", F.round(F.cume_dist().over(w_moved_at_month_operator_by_q), 3))
    .withColumn("warehouse_id_sum_unit_price", F.sum("unit_price").over(w_warehouse_id))
)

combined_details = (
    ranked.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price_adj",
        "s.warehouse_id_sum_unit_price",
        F.col("sk.labels").alias("sku_labels"),
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

with_children_moves = (
    combined_details.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.sku_labels",
        "s.sku_code",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.operator").alias("move_operator"),
    )
)

exploded_detail_moves = (
    with_children_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.move_sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.sku_code",
        "s.move_id",
        "s.move_moved_at",
        "s.move_sku_id",
        "s.move_operator",
        F.col("st.id").alias("move_id_2"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.operator").alias("move_operator_2"),
    )
)

result = (
    exploded_detail_moves
    .select(
        "id",
        "sku_id",
        "qty",
        "sku_code",
        "move_id",
        "move_operator",
        "move_id_2",
        "move_warehouse_id",
        "move_operator_2",
        F.abs(F.col("qty") - 268).alias("qty_distance"),
        F.coalesce(F.col("move_operator"), F.lit("UNKNOWN")).alias("move_operator_or_unknown"),
    )
    .orderBy(F.col("move_id").desc())
)
