from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")

scoped = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-03-02").cast("date")) | (F.col("operator").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
    )
)

enriched = (
    scoped
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "move_type_ref",
        F.when(F.col("operator").isin("op_dew", "op_chan"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
        F.concat_ws(": ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

enriched_warehouse = (
    enriched.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") > 4912), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.operator_group",
        "s.move_type_operator_label",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

windowed = (
    enriched_warehouse
    .select(
        "*",
        F.round(F.col("sku_weight_kg") / F.nullif(F.sum("sku_weight_kg").over(w_operator), F.lit(0)), 4).alias("sku_weight_kg_share_of_operator"),
        F.round(F.col("warehouse_capacity") / F.nullif(F.sum("warehouse_capacity").over(w_operator), F.lit(0)), 4).alias("warehouse_capacity_share_of_operator"),
    )
)

result = (
    windowed
    .rollup("sku_id", "warehouse_id")
    .agg(
        F.grouping("sku_id").alias("is_total_sku_id"),
        F.min("warehouse_capacity").alias("min_warehouse_capacity"),
    )
)
