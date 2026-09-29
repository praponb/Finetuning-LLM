from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("k", "attr_value"))
)

result = (
    exploded
    .groupBy("k", F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count(F.when(F.col("k") == "gift", 1)).alias("gift_count"),
        F.countDistinct("k").alias("distinct_ks"),
        F.max("created_at").alias("last_created_at"),
        F.count_if(F.col("k") == "gift").alias("gift_events"),
    )
    .filter(F.col("gift_events") >= 1)
    .orderBy(F.col("gift_count").desc_nulls_first())
)
