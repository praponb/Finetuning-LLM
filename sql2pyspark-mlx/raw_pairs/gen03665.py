from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("num_products"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "left")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
        F.col("supplier_totals.num_products"),
        F.col("supplier_totals.min_cost"),
    )
)
