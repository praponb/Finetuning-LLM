from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .filter(F.col("p.price").between(630, 660))
    .select(F.col("p.id"), F.col("p.stock_qty"), F.col("p.price"), F.col("s.city"))
)
