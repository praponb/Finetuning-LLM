from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(~F.col("p.brand").isin(["Initech", "Acme", "Globex"]) & F.col("p.stock_qty").between(172, 300))
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
)
