from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.avg_quantity") > 200)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.col("pt.avg_quantity"),
    )
    .orderBy(F.col("id").desc(), "supplier_id")
)
