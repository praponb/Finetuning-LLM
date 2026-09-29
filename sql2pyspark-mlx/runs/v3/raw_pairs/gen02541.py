from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(~F.col("s.supplier_name").like("Eur%"))
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
)
