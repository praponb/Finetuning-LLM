from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(~F.col("p.brand").isin(["Stark", "Initech", "Acme"]))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.min(F.col("p.launch_date")).alias("first_launch_date"),
        F.sum(F.when(F.col("p.category") == "Electronics", 1).otherwise(0)).alias("electronics_count"),
    )
    .orderBy("id")
)
