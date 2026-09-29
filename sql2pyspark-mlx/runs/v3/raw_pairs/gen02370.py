from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.cost") > 330)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("s.country"),
        F.col("s.rating"),
        (F.col("p.price") + F.col("p.cost")).alias("margin"),
    )
)
