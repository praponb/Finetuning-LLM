from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(~F.col("channel").isin(["mobile", "partner", "web"]))
    .groupBy("store_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.sqft"), F.col("s.city"), F.col("ss.status_list"))
    .orderBy(F.col("sqft").desc())
)
