from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .select(
        F.col("s.country"),
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.coalesce(F.col("p.product_name"), F.lit("unassigned")).alias("product_name_filled"),
    )
    .orderBy("launch_date")
)
