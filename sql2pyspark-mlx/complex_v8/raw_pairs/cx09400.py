from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("operator") != "op_ann")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
    )
)

with_parent_sku = (
    prepared_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.moved_at_hour",
        "s.qty_minus_unit_price",
        "s.moved_at_day",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

result = (
    with_parent_sku
    .groupingSets(
        [["operator", "sku_id"], ["operator"], ["sku_id"]],
        "operator", "sku_id",
    )
    .agg(
        F.grouping("operator").alias("is_total_operator"),
        F.round(F.stddev("qty_minus_unit_price"), 2).alias("stddev_qty_minus_unit_price"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.min("moved_at_hour").alias("first_moved_at_hour"),
    )
    .select(
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        "sku_id",
        "is_total_operator",
        "stddev_qty_minus_unit_price",
        "distinct_warehouse_ids",
        "first_moved_at_hour",
    )
    .orderBy(F.col("operator").asc_nulls_last(), F.col("sku_id").asc_nulls_last())
)
