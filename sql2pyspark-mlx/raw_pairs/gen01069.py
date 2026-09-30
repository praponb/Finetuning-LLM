from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

result = (
    exploded
    .groupBy("customer_id", "code")
    .agg(
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("code")).alias("code_list_2"),
        F.sort_array(F.collect_set("code")).alias("code_list_3"),
        F.count_if(F.col("code") == "SPRING10").alias("spring10_events"),
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_count"),
    )
    .orderBy(F.col("spring10_events").desc())
)
