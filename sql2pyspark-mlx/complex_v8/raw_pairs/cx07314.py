from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_sku_weight_kg_running = Window.orderBy("sku_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_sku_weight_kg_ties = Window.orderBy("sku_weight_kg")
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_all_by_sku_weight_kg = Window.orderBy("sku_weight_kg", "id")

base_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.abs(F.col("unit_price") - 258.6).alias("unit_price_distance"),
    )
)

enriched_sku = (
    base_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.weight_kg") > 4.7)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.unit_price_distance",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
    )
)

filtered = (
    enriched_sku.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") < 387), "left_semi")
)

with_trends = (
    filtered
    .select(
        "*",
        F.first("unit_price_distance", ignorenulls=True).over(w_all_by_sku_weight_kg_running).alias("first_known_unit_price_distance"),
        F.dense_rank().over(w_all_by_sku_weight_kg_ties).alias("sku_weight_kg_dense_rank"),
        F.count(F.lit(1)).over(w_operator_warehouse_id).alias("rows_in_operator_warehouse_id"),
        F.lag("sku_weight_kg").over(w_all_by_sku_weight_kg).alias("prev_sku_weight_kg"),
        F.round(F.col("unit_price_distance") - F.avg("unit_price_distance").over(w_operator_warehouse_id), 2).alias("unit_price_distance_vs_operator_warehouse_id_avg"),
    )
)

annotated = (
    with_trends
    .filter(F.upper(F.trim(F.col("sku_code"))).like("T%"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "unit_price_distance",
        "sku_weight_kg",
        "first_known_unit_price_distance",
        "sku_weight_kg_dense_rank",
        "unit_price_distance_vs_operator_warehouse_id_avg",
        (F.col("sku_weight_kg") + F.col("unit_price_distance")).alias("sku_weight_kg_plus_unit_price_distance"),
        F.round(F.col("sku_weight_kg") / F.nullif(F.col("unit_price_distance"), F.lit(0)), 3).alias("sku_weight_kg_per_unit_price_distance"),
        F.concat(F.col("sku_code"), F.lit("#"), F.col("sku_weight_kg_dense_rank").cast("string")).alias("sku_code_ref"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

with_counts = (
    annotated.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    with_counts
    .groupBy("move_count", "sku_id")
    .pivot("operator", ["op_bee", "op_dew", "op_eve"])
    .agg(F.min("unit_price_distance"))
    .orderBy("move_count", "sku_id")
)
