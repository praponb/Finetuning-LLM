from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("category").isin(["Sports", "Home"]))
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("P"), F.col("s.id") == F.col("P.supplier_id"), "left")
    .filter(F.col("P.min_cost") >= 540)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.country"),
        F.col("P.stddev_cost"),
        F.col("P.min_cost"),
    )
    .orderBy("id", F.col("stddev_cost").desc())
)
