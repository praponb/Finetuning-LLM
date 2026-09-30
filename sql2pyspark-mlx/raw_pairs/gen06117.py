from pyspark.sql import functions as F

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty") == 262), "left_anti")
)

result = (
    eligible
    .select("id", "supplier_name", "country", "city", "rating")
    .orderBy("id")
)
