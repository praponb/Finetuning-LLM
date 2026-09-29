from pyspark.sql import functions as F

src = df_orders.filter(F.col("store_id").isNotNull() | (F.col("channel") != "mobile"))

agg = (
    src
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
    )
)

result = (
    agg
    .filter(F.col("completed_count") > 68)
    .select("store_id", "completed_count")
    .orderBy(F.col("store_id").desc())
)
