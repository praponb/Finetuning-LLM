from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.sum(F.when(F.col("o.channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
    .orderBy("city", F.col("partner_count").desc())
    .limit(50)
)
