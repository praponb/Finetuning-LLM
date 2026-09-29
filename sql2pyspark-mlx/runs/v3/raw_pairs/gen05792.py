from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.brand"), F.col("p.supplier_id"))
    .agg(F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"))
)
