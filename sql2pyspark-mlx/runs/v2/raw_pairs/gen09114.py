from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .filter(F.col("s.city").isin(["Berlin", "Singapore"]) & (F.col("p.cost") > 600))
    .select(
        F.col("i.product_id"),
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.substring(F.col("s.country"), 1, 3).alias("country_prefix"),
    )
)
