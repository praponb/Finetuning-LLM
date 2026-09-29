from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

move_side = (
    base
    .select(
        "id",
        F.col("warehouse_id").alias("key_id"),
        F.col("move_type").alias("category"),
    )
)

warehouse_rows = (
    df_warehouses
    .filter((F.col("capacity").between(2614, 11506)) & (F.upper(F.trim(F.col("wh_code"))).like("B%")))
    .select("capacity", "id", "wh_code")
)

move_rows = (
    df_stock_moves
    .filter((F.col("unit_price") <= 337.6) & (F.col("operator") == "op_eve"))
    .select("warehouse_id", "qty", "operator")
)

unioned = (
    move_side
    .union(warehouse_rows)
    .union(move_rows)
)

result = (
    unioned
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"), F.sum("key_id").alias("total_key_id"))
    .orderBy("category")
)
