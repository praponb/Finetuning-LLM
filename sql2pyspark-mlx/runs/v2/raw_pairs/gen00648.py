from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("suppliers.country").isin(["US", "TH"]))
    .select(
        F.col("suppliers.city"),
        F.col("suppliers.supplier_name"),
        F.col("products.brand"),
        F.col("products.launch_date"),
        F.col("products.price"),
        F.array_join(F.col("products.tags"), ", ").alias("tags_text"),
    )
)
