from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("operator").isin("op_dew", "op_chan", "op_ann"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean"),
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
    )
)

combined_details_sku = (
    prepared_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category") == "Apparel"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.moved_at_month",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

joined = (
    combined_details_sku.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.city") == "Bangkok")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at_month",
        "s.sku_category",
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    joined
    .groupBy("moved_at_month")
    .pivot("sku_category", ["Electronics", "Beauty"])
    .agg(F.sum("qty"))
)
