from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("price") <= 430)
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("stock_qty").alias("unique_brands"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "left")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("supplier_totals.unique_brands"),
        F.col("supplier_totals.avg_cost"),
    )
    .orderBy(F.col("avg_cost").desc())
)
