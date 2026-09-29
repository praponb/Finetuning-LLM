from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.min(F.col("p.launch_date")).alias("first_launch_date"),
    )
    .orderBy("product_count", "first_launch_date")
)
