from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_category = Window.partitionBy("operator", "sku_category")
w_move_type_sku_category_by_sk = Window.partitionBy("move_type", "sku_category").orderBy("sku_weight_kg")

filtered_moves = (
    df_stock_moves
    .filter((F.col("qty").between(139, 468)) | (F.hour(F.col("moved_at")).between(6, 12)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

derived = (
    filtered_moves
    .select(
        "*",
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
        F.abs(F.col("unit_price") - 299.1).alias("unit_price_distance"),
    )
)

with_details = (
    derived.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_clean",
        "s.moved_at_day",
        "s.unit_price_distance",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

with_activity = (
    with_details.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator").isin("op_dew", "op_chan")), "left_semi")
)

enriched = (
    with_activity
    .select(
        "*",
        F.when(F.col("sku_category").isin("Beauty", "Electronics"), "group_a").when(F.col("sku_category") == "Apparel", "group_b").otherwise("other").alias("sku_category_group"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.size(F.collect_set("sku_id").over(w_operator_sku_category)).alias("distinct_sku_id_in_operator_sku_category"),
        F.round(F.percent_rank().over(w_move_type_sku_category_by_sk), 3).alias("sku_weight_kg_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        F.col("distinct_sku_id_in_operator_sku_category").alias("m_distinct_sku_id_in_operator_sku_category"),
        F.col("warehouse_capacity").alias("warehouse_capacity_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_distinct_sku_id_in_operator_sku_category", "warehouse_capacity_v"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
