from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

result = (
    lines
    .rollup("k")
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count_if(F.col("k") == "gift").alias("gift_events"),
        F.count(F.when(F.col("k") == "utm_source", 1)).alias("utm_source_count"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.count_if(F.col("k") == "utm_source").alias("utm_source_events"),
    )
    .orderBy(F.col("utm_source_count").desc_nulls_first())
)
