from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("status") == "COMPLETED")
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.opened_date"), F.col("s.sqft"), F.col("ss.pct_partner"))
    .orderBy(F.col("sqft").desc())
)
