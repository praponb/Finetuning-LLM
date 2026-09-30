from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("P"), F.col("s.id") == F.col("P.supplier_id"), "inner")
    .filter(F.col("P.min_cost") > 210)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("P.min_cost"),
        F.col("P.first_launch_date"),
    )
    .orderBy("first_launch_date", "id")
    .limit(1)
)
