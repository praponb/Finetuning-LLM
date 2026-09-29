from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date") <= "2024-05-19"), "left_anti")
)

result = kept.select("id", "supplier_name", "city", "rating")
