from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("products.brand"),
        F.col("suppliers.rating"),
        F.col("suppliers.city"),
        F.col("suppliers.country"),
        (F.datediff(F.current_date(), F.col("products.launch_date")) / 365).alias("years_since_launch_date"),
    )
)
