from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "created_at", F.explode_outer("coupon_codes").alias("element"))
)

result = (
    exploded
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"), "element")
    .agg(
        F.count_if(F.col("element") == "WELCOME5").alias("welcome5_events"),
        F.size(F.collect_set("element")).alias("num_elements"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("element")).alias("num_elements_2"),
        F.max("created_at").alias("last_seen"),
        F.max("created_at").alias("ended_at"),
    )
)
