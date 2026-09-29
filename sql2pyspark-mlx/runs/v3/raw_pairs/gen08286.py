from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("category").isin(["Fashion", "Sports", "Home"]))
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0))) / F.count("*")), 1).alias("pct_stark"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.pct_grocery") > 58)
    .select(F.col("s.supplier_name"), F.col("ss.pct_grocery"), F.col("ss.pct_stark"))
)
