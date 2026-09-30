from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "right")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .filter(F.col("products.cost").between(450, 490))
    .groupBy(F.date_trunc("MONTH", F.col("products.launch_date")).alias("launch_month"))
    .agg(F.sum(F.col("store_sales.units")).alias("total_units"))
    .filter(F.col("total_units") >= 220)
    .orderBy(F.col("total_units").desc())
)
