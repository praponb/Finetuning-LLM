from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("p.stock_qty") == 271) | (F.col("s.city") == "London"))
    .select(F.col("p.cost"), F.col("p.brand"), F.col("s.city"))
)
