from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull() & F.col("launch_date").between("2023-08-26", "2024-02-19"))
    .agg(
        F.count("*").alias("product_count"),
        F.countDistinct("price").alias("unique_suppliers"),
    )
    .orderBy(F.col("product_count").desc())
)
