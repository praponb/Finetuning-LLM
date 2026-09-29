from pyspark.sql import functions as F

move_rows = df_stock_moves.select("move_type")

sku_rows = (
    df_skus
    .filter((F.col("category") == "Beauty") & (F.lower(F.col("sku_code")).like("%ty%")))
    .select("sku_code")
)

result = move_rows.subtract(sku_rows)
