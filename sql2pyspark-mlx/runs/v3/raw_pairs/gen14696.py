from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(~((F.col("p.price") <= 1080) | (F.length(F.col("p.product_name")) >= 12)))
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
    )
    .orderBy("rating", "id")
    .limit(1)
)
