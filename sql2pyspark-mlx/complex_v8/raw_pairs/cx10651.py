from pyspark.sql import functions as F

warehouse_rows = df_warehouses.select("city")

sku_side = (
    df_skus
    .filter((F.col("category").isin("Apparel", "Beauty")) & (F.col("unit_cost") < 302.5))
    .select("sku_code")
)

result = warehouse_rows.exceptAll(sku_side)
