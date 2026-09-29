from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter(F.col("qty") > 102)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.when(F.col("operator").isin("op_chan", "op_eve"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group"),
    )
)

move_rows = (
    base_moves
    .select(
        F.col("sku_id").alias("entity_id"),
        F.col("operator_group").alias("name"),
    )
)

warehouse_side = (
    df_warehouses
    .filter(F.col("opened_on") < F.lit("2025-02-14").cast("date"))
    .select("capacity", "city")
)

result = (
    move_rows
    .subtract(warehouse_side)
    .orderBy(F.col("name").desc(), "entity_id")
)
