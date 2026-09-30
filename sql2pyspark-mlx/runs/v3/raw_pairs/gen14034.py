from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.stddev_cost") > 77)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.rating"),
        F.col("t.stddev_cost"),
        F.col("t.umbrella_count"),
    )
    .orderBy("city")
)
