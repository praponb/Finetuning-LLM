from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty") < 131)
    .groupBy("supplier_id")
    .agg(
        F.count("price").alias("price_count"),
        F.sum(F.when(F.col("category") == "Sports", F.col("cost")).otherwise(0)).alias("sports_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.price_count") >= 4)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("pbs.price_count"),
        F.col("pbs.sports_cost"),
    )
    .orderBy(F.col("sports_cost").desc())
)
