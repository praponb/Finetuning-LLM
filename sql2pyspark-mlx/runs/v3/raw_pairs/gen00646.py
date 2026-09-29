from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.countDistinct(F.col("o.discount")).alias("unique_customers"),
        F.count(F.when(F.col("o.status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
    .orderBy("unique_customers")
)
