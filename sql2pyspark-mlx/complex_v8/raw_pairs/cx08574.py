from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter(F.month(F.col("moved_at")).isin(1, 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
    )
)

derived = (
    cleaned_moves
    .select(
        "*",
        F.substring(F.trim(F.col("move_type_prefix")), 1, 3).alias("move_type_prefix_prefix"),
        F.length(F.trim(F.col("move_type_prefix"))).alias("move_type_prefix_len"),
    )
)

filtered = (
    derived.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.move_type").isin("TRANSFER", "ADJUST")) & (F.col("st.warehouse_id").isNotNull()), "left_anti")
)

qualified = (
    filtered.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.upper(F.trim(F.col("sk.sku_code"))).like("A%")), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "move_type_prefix",
        "move_type_prefix_prefix",
        "move_type_prefix_len",
        F.when(F.col("operator").isin("op_bee", "op_ann"), "group_a").when(F.col("operator") == "op_eve", "group_b").otherwise("other").alias("operator_group"),
        F.least(F.col("move_type_prefix_len"), F.col("qty")).alias("least_move_type_prefix_len_qty"),
    )
    .orderBy("qty")
)
