from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("key", "attr_value"))
)

result = (
    exploded
    .groupBy("key", F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.min("created_at").alias("first_seen"),
    )
    .orderBy(F.col("key").asc_nulls_first())
)
