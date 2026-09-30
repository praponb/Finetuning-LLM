from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_completed"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .filter(F.col("S.pct_completed") > 61)
    .select(
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("S.pct_completed"),
        F.col("S.row_count"),
    )
    .orderBy(F.col("sqft").desc())
)
