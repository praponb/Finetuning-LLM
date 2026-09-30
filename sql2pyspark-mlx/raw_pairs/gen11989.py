from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.col("p.launch_date").between("2024-07-01", "2025-03-29"))
    .groupBy(
        F.col("s.country"),
        F.when(F.col("s.rating") >= 4.0, "high").when(F.col("s.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("num_store_sales"),
        (F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"),
    )
    .filter(F.col("num_store_sales") > 1)
)
