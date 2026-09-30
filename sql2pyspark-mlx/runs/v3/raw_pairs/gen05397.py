from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.stock_qty") > 183)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
)
