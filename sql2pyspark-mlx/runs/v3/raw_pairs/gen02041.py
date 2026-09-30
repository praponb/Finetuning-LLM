from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.quantity_range") > 68)
    .select(F.col("p.launch_date"), F.col("p.category"), F.col("pt.quantity_range"))
    .orderBy("quantity_range")
)
