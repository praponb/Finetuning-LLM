from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.supplier_name").like("%Siam%"))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.count(F.when(F.col("p.category") == "Home", 1)).alias("home_count"),
        F.min(F.col("p.launch_date")).alias("first_launch_date"),
    )
)
