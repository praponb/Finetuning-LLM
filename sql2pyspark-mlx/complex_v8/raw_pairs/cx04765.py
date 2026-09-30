from pyspark.sql import functions as F

valid_warehouses = (
    df_warehouses
    .filter(F.col("city") == "Bangkok")
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.abs(F.col("capacity") - 11005).alias("capacity_distance"),
    )
)

warehouse_side = (
    valid_warehouses
    .filter(F.col("capacity_distance").between(2000, 19000))
    .select("id")
)

move_rows = (
    df_stock_moves
    .filter(F.month(F.col("moved_at")).isin(1, 3))
    .select("sku_id")
)

result = (
    warehouse_side
    .intersect(move_rows)
    .orderBy(F.col("id").desc())
)
