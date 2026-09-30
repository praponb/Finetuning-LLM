from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "Bangkok")
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.sum(F.when(F.col("p.brand") == "Stark", 1).otherwise(0)).alias("stark_count"),
    )
    .filter(F.col("product_count") >= 3)
)
