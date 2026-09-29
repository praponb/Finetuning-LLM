from pyspark.sql import functions as F

scoped_warehouses = (
    df_warehouses
    .filter(F.length(F.trim(F.col("wh_code"))) >= 7)
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
    )
)

warehouse_side = scoped_warehouses.select("id")

move_side = (
    df_stock_moves
    .filter((F.col("operator") == "op_eve") & (F.col("qty").between(112, 357)))
    .select("sku_id")
)

result = warehouse_side.subtract(move_side)
