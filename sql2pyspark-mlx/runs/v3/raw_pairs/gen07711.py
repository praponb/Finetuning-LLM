from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("city").isin(["New York", "Bangkok", "London"])).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.launch_date") <= "2025-08-01")
    .select(F.col("p.id"), F.col("p.brand"), F.col("p.stock_qty"))
)
