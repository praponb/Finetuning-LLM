from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.avg("cost").alias("avg_cost"),
        (F.max("price") - F.min("price")).alias("price_range"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("P"), F.col("s.id") == F.col("P.supplier_id"), "left")
    .filter(F.col("P.price_range") > 57)
    .select(
        F.col("s.country"),
        F.col("s.city"),
        F.col("P.avg_cost"),
        F.col("P.price_range"),
    )
)
