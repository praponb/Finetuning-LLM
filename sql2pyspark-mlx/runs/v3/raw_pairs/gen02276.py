from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.avg("price").alias("avg_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(F.col("s.rating"), F.col("pbs.min_cost"), F.col("pbs.avg_price"))
    .orderBy(F.col("min_cost").desc())
)
