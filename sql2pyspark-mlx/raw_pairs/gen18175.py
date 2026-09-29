from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("launch_date").between("2023-04-11", "2025-02-01")).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("ss.units").isNotNull())
    .select(F.col("ss.id"), F.col("ss.store_id"))
)
