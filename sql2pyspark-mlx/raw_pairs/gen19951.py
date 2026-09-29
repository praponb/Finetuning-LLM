from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("value"))

result = (
    lines
    .groupBy("customer_id", "value")
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("value")).alias("value_list"),
        F.count_if(F.col("value") == "VIP").alias("vip_events"),
        F.size(F.collect_set("value")).alias("num_values"),
        F.count_if(F.col("value") == "VIP").alias("vip_events_2"),
    )
    .filter(F.col("vip_events_2") > 1)
    .orderBy(F.col("vip_events").asc_nulls_last())
)
