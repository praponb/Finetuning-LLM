from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.brand").isNull())
    .groupBy(F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.max(F.col("p.launch_date")).alias("latest_launch_date"),
        F.max(F.col("p.price")).alias("max_price"),
    )
    .orderBy("latest_launch_date")
)
