from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("units").between(12, 38))
    .groupBy("product_id")
    .agg((F.max("units") - F.min("units")).alias("units_range"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.cost"), F.col("pt.units_range"))
    .orderBy("cost")
)
