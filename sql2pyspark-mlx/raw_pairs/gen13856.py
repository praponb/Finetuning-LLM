from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode_outer("coupon_codes").alias("value"))

result = (
    flattened
    .rollup("value")
    .agg(
        F.size(F.collect_set("value")).alias("num_values"),
        F.count_if(F.col("value") == "VIP").alias("vip_events"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("value").desc())
)
