from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.when(F.col("p.brand") == "Globex", 1)).alias("globex_count"),
        F.count(F.col("p.cost")).alias("cost_count"),
    )
    .orderBy(F.col("globex_count").desc(), "cost_count")
)
