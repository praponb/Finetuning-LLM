from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_to_date_by_move_qty = Window.partitionBy(F.to_date("moved_at")).orderBy("move_qty", "id", "move_id").rowsBetween(-2, -1)
w_moved_at_to_date_by_move_qty_2 = Window.partitionBy(F.to_date("moved_at")).orderBy("move_qty", "id", "move_id")
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")

scoped_moves = (
    df_stock_moves
    .filter(F.col("operator").isin("op_eve", "op_ann", "op_bee"))
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
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

screened = (
    scoped_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id") <= 10), "left_anti")
)

with_children = (
    screened.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_prefix",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
    )
)

ranked = (
    with_children
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "move_id",
        "move_unit_price",
        "move_qty",
        F.sum("move_unit_price").over(w_moved_at_to_date_by_move_qty).alias("move_unit_price_sum_2_prior"),
        F.round(F.cume_dist().over(w_moved_at_to_date_by_move_qty_2), 3).alias("move_qty_cume_dist"),
        F.size(F.collect_set("move_id").over(w_warehouse_id_sku_id)).alias("distinct_move_id_in_warehouse_id_sku_id"),
    )
)

per_group = (
    ranked
    .groupBy(
        "distinct_move_id_in_warehouse_id_sku_id",
        F.when(F.col("move_qty") >= 422, "A").when(F.col("move_qty") >= 112, "B").otherwise("C").alias("move_qty_tier"),
        "operator",
    )
    .agg(
        F.countDistinct("operator").alias("distinct_operators"),
        F.count("*").alias("n_rows"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

enriched = (
    per_group
    .filter(F.hour(F.col("last_moved_at")).between(6, 10))
    .select(
        "distinct_move_id_in_warehouse_id_sku_id",
        "move_qty_tier",
        "operator",
        "n_rows",
        "last_moved_at",
        F.least(F.col("distinct_operators"), F.col("n_rows")).alias("least_distinct_operators_n_rows"),
        (F.col("n_rows") - F.col("distinct_operators")).alias("n_rows_minus_distinct_operators"),
    )
)

scored = (
    enriched
    .select(
        "distinct_move_id_in_warehouse_id_sku_id",
        "move_qty_tier",
        "operator",
        "n_rows",
        "least_distinct_operators_n_rows",
        "n_rows_minus_distinct_operators",
        F.initcap(F.trim(F.col("move_qty_tier"))).alias("move_qty_tier_clean"),
    )
)

result = (
    scored
    .groupBy("operator")
    .pivot("move_qty_tier", ["A", "C"])
    .agg(F.avg("least_distinct_operators_n_rows"))
    .orderBy("operator")
)
