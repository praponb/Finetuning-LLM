from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.total_cost") > 4200)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("t.min_cost"),
        F.col("t.total_cost"),
    )
    .orderBy(F.col("rating").desc())
)
