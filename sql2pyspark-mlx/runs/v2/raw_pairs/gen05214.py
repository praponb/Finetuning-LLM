from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("discount")).otherwise(0)).alias("mobile_discount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(F.col("s.sqft"), F.col("t.total_amount"), F.col("t.mobile_discount"))
    .orderBy("sqft", "mobile_discount")
)
