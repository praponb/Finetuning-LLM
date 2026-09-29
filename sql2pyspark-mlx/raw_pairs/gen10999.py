from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.year("launch_date") == 2025) & (F.col("launch_date") <= "2025-09-01")) | (F.col("stock_qty") < 291))
    .select("id", "launch_date", "brand", "supplier_id")
    .orderBy("supplier_id")
)
