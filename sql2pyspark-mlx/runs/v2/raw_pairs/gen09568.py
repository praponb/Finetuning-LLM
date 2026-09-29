from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
        F.sum(F.when(F.col("o.channel") == "partner", F.col("o.amount")).otherwise(0)).alias("partner_amount"),
    )
    .filter(F.col("partner_amount") >= 52)
    .orderBy("partner_amount", "region")
    .limit(10)
)
