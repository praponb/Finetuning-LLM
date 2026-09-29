from pyspark.sql import functions as F

result = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.year(F.col("c.launch_date")) >= 2023), "left_anti")
)
