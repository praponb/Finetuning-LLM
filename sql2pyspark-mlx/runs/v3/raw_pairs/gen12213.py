from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.brand").isin(["Umbrella", "Stark"]))
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("p.product_name"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.datediff(F.current_date(), F.col("p.launch_date")).alias("days_since_launch_date"),
    )
)
