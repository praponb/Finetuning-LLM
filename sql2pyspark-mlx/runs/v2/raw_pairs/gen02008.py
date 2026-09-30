from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("quantity").between(113, 137))
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.max_quantity") > 180)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("pt.max_quantity"),
    )
    .orderBy("id", F.col("max_quantity").desc())
)
