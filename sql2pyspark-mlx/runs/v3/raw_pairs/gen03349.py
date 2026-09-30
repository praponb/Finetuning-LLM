from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isin(["Berlin", "Bangkok"]))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.avg(F.col("p.cost")).alias("avg_cost"),
        (F.max(F.col("p.price")) - F.min(F.col("p.price"))).alias("price_range"),
    )
)
