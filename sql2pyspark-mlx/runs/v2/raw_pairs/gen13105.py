from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter((F.col("p.cost") < 460) & (F.col("p.supplier_id") == 21))
    .select(F.col("p.cost"), F.col("p.brand"), F.col("ss.store_id"), F.col("ss.units"))
)
