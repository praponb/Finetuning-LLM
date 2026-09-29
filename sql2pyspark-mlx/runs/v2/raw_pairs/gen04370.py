from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Fashion", F.col("cost")).otherwise(0)).alias("fashion_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "left")
    .select(F.col("s.rating"), F.col("S.fashion_cost"))
)
