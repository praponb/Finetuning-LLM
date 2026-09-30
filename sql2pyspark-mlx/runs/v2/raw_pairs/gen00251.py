from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(F.col("suppliers.city"))
    .agg(
        F.count("*").alias("product_count"),
        F.sum(F.when(F.col("products.brand") == "Stark", F.col("products.price")).otherwise(0)).alias("stark_price"),
    )
    .orderBy("stark_price")
)
