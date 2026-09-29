from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.brand").isin(["Globex", "Acme"]))
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.round(F.avg(F.col("p.price")), 2).alias("avg_price"),
    )
)
