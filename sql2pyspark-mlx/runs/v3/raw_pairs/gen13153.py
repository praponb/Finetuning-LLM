from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("status") == "RETURNED")
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
        F.countDistinct("amount").alias("unique_stores"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("t.pct_partner"),
        F.col("t.unique_stores"),
    )
    .orderBy("id")
)
