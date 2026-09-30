from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.stock_qty").between(112, 364) | F.col("s.rating").between(2.7, 3.9))
    .select(
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("p.brand"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
)
