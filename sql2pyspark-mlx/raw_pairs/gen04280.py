from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(F.col("suppliers.city"), F.col("suppliers.country"))
    .agg(
        F.sum(F.when(F.col("products.brand") == "Stark", 1).otherwise(0)).alias("stark_count"),
    )
    .filter(F.col("stark_count") >= 32)
)
