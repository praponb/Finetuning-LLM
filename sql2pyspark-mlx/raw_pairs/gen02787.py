from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter((F.year(F.col("p.launch_date")) == 2023) & ~F.col("s.country").isin(["DE", "UK"]))
    .select(
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.col("s.rating"),
        F.col("s.country"),
    )
)
