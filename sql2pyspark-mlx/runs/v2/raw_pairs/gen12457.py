from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("category") == "Electronics")
    .withColumn("stock_qty_minus_cost", (F.col("stock_qty") - F.col("cost")))
)

agg = (
    recent_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("cost").alias("unique_suppliers"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0))) / F.count("*")), 1).alias("pct_globex"),
    )
)

result = (
    agg
    .filter(F.col("unique_suppliers") >= 5)
    .select("supplier_id", "unique_suppliers", "pct_globex")
)
