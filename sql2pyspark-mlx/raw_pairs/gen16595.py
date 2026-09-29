from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("coupon_codes_count").desc_nulls_first())
w2 = Window.partitionBy("attr_key", "customer_id").orderBy(F.col("coupon_codes_count").desc_nulls_last())
w_grp = Window.partitionBy("attr_key").orderBy(F.col("coupon_codes_count").desc_nulls_first())

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

filtered = (
    flattened
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("gift"))))
    .withColumn("sorted_coupon_codes", F.sort_array("coupon_codes"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

valid = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "attr_key",
        "attr_value",
        "sorted_coupon_codes",
        F.size("coupon_codes").alias("coupon_codes_count"),
    )
)

scored = (
    valid
    .filter(F.col("coupon_codes_count") <= 57)
    .withColumn("coupon_codes_count_dense_rank", F.dense_rank().over(w))
    .withColumn("coupon_codes_count_cume_dist", F.cume_dist().over(w2))
    .withColumn("coupon_codes_count_percent_rank", F.percent_rank().over(w_grp))
)

result = (
    scored
    .groupBy(
        F.when(F.col("coupon_codes_count_dense_rank") >= 9, "LARGE").otherwise("SMALL").alias("coupon_codes_count_dense_rank_tier"),
        "customer_id",
    )
    .agg(
        F.count(F.when(F.col("attr_key") == "utm_source", 1)).alias("utm_source_count"),
        F.count("*").alias("cnt"),
        F.max("coupon_codes_count").alias("max_coupon_codes_count"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("cnt").desc())
)
