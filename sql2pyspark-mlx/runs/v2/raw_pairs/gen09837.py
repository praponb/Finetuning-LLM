from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("price") < 630)
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0))) / F.count("*")), 1).alias("pct_electronics"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.pct_electronics") > 75)
    .select(F.col("s.supplier_name"), F.col("t.pct_electronics"))
    .orderBy("supplier_name")
)
