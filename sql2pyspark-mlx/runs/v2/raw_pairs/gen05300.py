from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "right")
    .filter(F.col("products.launch_date").between("2024-09-05", "2024-11-01"))
    .groupBy(F.col("suppliers.country"), F.col("suppliers.city"))
    .agg(F.countDistinct(F.col("products.cost")).alias("unique_categorys"))
    .filter(F.col("unique_categorys") > 3)
)
