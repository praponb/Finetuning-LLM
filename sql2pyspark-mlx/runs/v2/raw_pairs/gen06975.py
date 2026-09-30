from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.max(F.col("p.cost")).alias("max_cost"),
        F.sum(F.when(F.col("p.category") == "Home", F.col("p.cost")).otherwise(0)).alias("home_cost"),
    )
)
