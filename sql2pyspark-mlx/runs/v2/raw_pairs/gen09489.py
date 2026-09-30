from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("channel") == "store")
    .groupBy("store_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.total_amount") >= 8300)
    .select(
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("t.total_amount"),
        F.col("t.partner_count"),
    )
    .orderBy(F.col("city").desc())
)
