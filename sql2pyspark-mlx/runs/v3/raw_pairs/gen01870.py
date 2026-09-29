from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.quantity").between(186, 239))
    .groupBy(
        F.quarter(F.col("p.launch_date")).alias("launch_quarter"),
        F.col("p.category"),
    )
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
    )
    .orderBy(F.col("total_quantity").desc(), F.col("category").desc(), "launch_quarter")
    .limit(50)
)
