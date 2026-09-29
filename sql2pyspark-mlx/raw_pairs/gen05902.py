from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", F.col("cost")).otherwise(0)).alias("initech_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(F.col("s.rating"), F.col("pbs.initech_cost"))
    .orderBy("rating", F.col("initech_cost").desc())
)
