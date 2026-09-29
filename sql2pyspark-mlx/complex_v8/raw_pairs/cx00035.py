from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("operator") != "op_chan")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_bee", "op_ann"), "group_a").when(F.col("operator") == "op_chan", "group_b").otherwise("other").alias("operator_group"),
    )
)

with_details_sku = (
    prepared_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

result = (
    with_details_sku
    .cube("sku_category", "sku_id")
    .agg(F.min("qty").alias("min_qty"))
    .orderBy(
        F.col("sku_category").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
