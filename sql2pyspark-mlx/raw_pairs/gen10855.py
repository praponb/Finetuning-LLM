from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"), F.col("s.supplier_name"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        (F.max(F.col("p.cost")) - F.min(F.col("p.cost"))).alias("cost_range"),
        F.sum(F.when(F.col("p.category") == "Electronics", F.col("p.price")).otherwise(0)).alias("electronics_price"),
    )
)
