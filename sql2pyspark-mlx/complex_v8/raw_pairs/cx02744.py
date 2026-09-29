from pyspark.sql import functions as F

prepared = (
    df_warehouses
    .filter(F.col("wh_code").isNotNull())
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
    )
)

warehouse_side = (
    prepared
    .filter(F.col("capacity") < 11005)
    .select("id")
)

move_side = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("operator").isin("op_dew", "op_eve", "op_chan")))
    .select("sku_id")
)

result = warehouse_side.subtract(move_side)
