from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("p.category").isin(["Sports", "Home"]), "tier_1").otherwise("other").alias("category_group"),
    )
    .agg(F.sum((F.col("oi.quantity") * F.col("oi.unit_price"))).alias("revenue"))
    .orderBy(F.col("category_group").desc(), F.col("revenue").desc())
)
