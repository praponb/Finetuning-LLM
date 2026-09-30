from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.year(F.col("opened_on")) == 2025) & (F.col("city") != "Bangkok"))
    .select("wh_code")
)

move_side = (
    df_stock_moves
    .filter((F.col("unit_price").between(37.6, 323.9)) & (F.col("sku_id").isNotNull()))
    .select("operator")
)

result = (
    warehouse_side
    .intersectAll(move_side)
    .orderBy(F.col("wh_code").desc())
)
