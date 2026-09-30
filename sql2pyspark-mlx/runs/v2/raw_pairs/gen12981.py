from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.price").between(340, 490))
    .groupBy(F.col("s.country"))
    .agg(
        F.sum(F.when(F.col("p.category") == "Electronics", 1).otherwise(0)).alias("electronics_count"),
    )
    .orderBy("electronics_count")
)
