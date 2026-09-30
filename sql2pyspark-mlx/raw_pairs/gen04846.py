from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
        F.sum(F.when(F.col("p.category") == "Home", F.col("p.cost")).otherwise(0)).alias("home_cost"),
    )
)
