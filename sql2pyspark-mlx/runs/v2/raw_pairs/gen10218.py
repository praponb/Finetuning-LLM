from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sports"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.pct_sports") >= 77)
    .select(F.col("s.id"), F.col("s.city"), F.col("t.pct_sports"))
    .orderBy(F.col("id").desc())
)
