from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_len_warehouse_id_by = Window.partitionBy("operator_len", "warehouse_id").orderBy("operator_len")
w_sku_id = Window.partitionBy("sku_id")
w_operator_len_warehouse_id_by_2 = Window.partitionBy("operator_len", "warehouse_id").orderBy("operator_len", "id")

recent_moves = (
    df_stock_moves
    .filter((F.col("unit_price").between(111.7, 330.0)) & (F.col("qty") > 148))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

derived = (
    recent_moves
    .select(
        "*",
        F.when(F.col("operator").isin("op_dew", "op_bee"), "group_a").when(F.col("operator") == "op_chan", "group_b").otherwise("other").alias("operator_group"),
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
    )
)

scored = (
    derived
    .select(
        "*",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

sequenced = (
    scored
    .select(
        "*",
        F.dense_rank().over(w_operator_len_warehouse_id_by).alias("operator_len_dense_rank"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.ntile(10).over(w_operator_len_warehouse_id_by_2).alias("operator_len_decile"),
    )
)

joined = (
    sequenced.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.weight_kg").between(6.5, 15.2))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.move_type_len",
        "s.operator_group",
        "s.unit_price_filled",
        "s.operator_len",
        "s.operator_or_unknown",
        "s.operator_len_dense_rank",
        "s.rows_in_sku_id",
        "s.operator_len_decile",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.category").alias("sku_category"),
        "sk.sku_code",
    )
)

exploded_detail_moves = (
    joined.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.unit_price_filled",
        "s.operator_len",
        "s.operator_len_dense_rank",
        "s.operator_len_decile",
        "s.sku_weight_kg",
        "s.sku_category",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

result = (
    exploded_detail_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "unit_price_filled",
        "operator_len",
        "operator_len_dense_rank",
        "operator_len_decile",
        "sku_weight_kg",
        "sku_category",
        "move_id",
        "move_operator",
        "move_move_type",
        F.concat_ws(" / ", F.col("sku_category"), F.col("move_move_type")).alias("sku_category_move_move_type_label"),
        F.round(F.col("sku_weight_kg") * 1.1, 2).alias("sku_weight_kg_adj"),
    )
    .orderBy(F.col("unit_price_filled").asc())
)
