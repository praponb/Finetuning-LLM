from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "created_at", "coupon_codes", F.explode("attributes").alias("k", "v"))
)

result = (
    exploded
    .groupBy(F.date_trunc("hour", "created_at").alias("hour_window"))
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
)
