from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "right")
    .filter(F.col("suppliers.city") == "London")
    .groupBy(F.col("suppliers.country"))
    .agg(F.min(F.col("products.launch_date")).alias("min_launch_date"))
    .orderBy("min_launch_date")
)
