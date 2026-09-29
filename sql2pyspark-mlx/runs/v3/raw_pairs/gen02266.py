from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0))) / F.count("*")), 1).alias("pct_electronics"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("t.pct_electronics"),
    )
    .orderBy(F.col("pct_electronics").desc(), F.col("rating").desc(), "id")
    .limit(10)
)
