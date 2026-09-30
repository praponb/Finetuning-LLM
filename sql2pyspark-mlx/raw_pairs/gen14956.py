from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("tag"))

result = (
    flattened
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
        F.count(F.when(F.col("tag") == "SPRING10", 1)).alias("spring10_rows"),
        F.count_if(F.col("tag") == "SPRING10").alias("spring10_events"),
        F.countDistinct("tag").alias("distinct_tags"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
