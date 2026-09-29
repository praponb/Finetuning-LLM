from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        F.sum(F.when(F.col("p.category") == "Home", 1).otherwise(0)).alias("home_count"),
        (F.max(F.col("p.price")) - F.min(F.col("p.price"))).alias("price_range"),
    )
    .orderBy("country")
    .limit(5)
)
