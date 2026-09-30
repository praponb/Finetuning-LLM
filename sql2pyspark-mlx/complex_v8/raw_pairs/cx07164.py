from pyspark.sql import functions as F

sku_rows = (
    df_skus
    .filter(~F.col("is_active"))
    .select("id")
)

warehouse_side = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("C%")) & (F.col("capacity") < 11005))
    .select("id")
)

result = sku_rows.subtract(warehouse_side)
