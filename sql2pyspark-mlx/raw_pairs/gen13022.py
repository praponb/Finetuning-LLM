from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        "customer_id",
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
    .orderBy("num_ks")
)
