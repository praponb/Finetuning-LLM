from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        (F.max(F.col("p.cost")) - F.min(F.col("p.cost"))).alias("cost_range"),
        F.count(F.col("p.price")).alias("price_count"),
    )
    .filter(F.col("price_count") > 3)
    .orderBy(F.col("cost_range").desc(), "country")
    .limit(3)
)
