from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fashion"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("pbs.pct_fashion"),
        F.col("pbs.stddev_price"),
    )
    .orderBy(F.col("stddev_price").desc())
)
