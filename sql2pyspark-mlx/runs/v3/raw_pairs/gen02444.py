from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("product_name").like("%Sma%"))
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("cost")).otherwise(0)).alias("sports_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.sports_cost") > 74)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.country"),
        F.col("pbs.sports_cost"),
    )
)
