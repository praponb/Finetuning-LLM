from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("moved_at").desc(), F.col("id").desc())

filtered = (
    df_stock_moves
    .filter(F.col("sku_id") >= 4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

deduped = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched_warehouse = (
    deduped.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.opened_on") >= F.lit("2024-11-25").cast("date")), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.operator_len",
        "s.days_since_moved_at",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

filtered_2 = (
    enriched_warehouse.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (~F.col("sk.is_active")), "left_semi")
)

with_parent_warehouse = (
    filtered_2.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.operator_len",
        "s.days_since_moved_at",
        "s.warehouse_city",
        "s.warehouse_capacity",
        "s.sku_unit_cost",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

exploded_detail = (
    with_parent_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") <= 326), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.days_since_moved_at",
        "s.sku_unit_cost",
        "s.warehouse_wh_code",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "days_since_moved_at",
        "sku_unit_cost",
        "move_id",
        "move_moved_at",
        "move_qty",
        "move_operator",
    )
    .orderBy(F.col("id").desc(), "warehouse_id")
)
