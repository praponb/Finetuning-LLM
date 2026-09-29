from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("stock_qty").isNotNull())
    .withColumn("cost_minus_stock_qty", (F.col("cost") - F.col("stock_qty")))
)

products_stats = (
    recent_products
    .groupBy("category", "supplier_id")
    .agg(F.sum("cost_minus_stock_qty").alias("total_cost_minus_stock_qty"))
)

result = (
    products_stats
    .filter(F.col("total_cost_minus_stock_qty") > 450)
    .select("category", "supplier_id", "total_cost_minus_stock_qty")
    .orderBy("category")
)
