from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.month("launch_date").isin([3, 10, 12])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.supplier_name").like("P%"))
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("s.rating"),
    )
    .orderBy("id", "country")
    .limit(10)
)
