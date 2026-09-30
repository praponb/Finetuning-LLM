from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("is_device").desc_nulls_first())
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy(F.year("created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.orderBy(F.col("coupon_codes_count").desc_nulls_first())
w5 = Window.orderBy(F.col("coupon_codes_count_2").asc_nulls_first())
w_ordered = Window.orderBy(F.col("coupon_codes_count").desc_nulls_last())
w7 = Window.orderBy(F.col("coupon_codes_count_percent_rank").desc_nulls_first())
w8 = Window.orderBy("coupon_codes_count_2_dense_rank")
w9 = Window.orderBy(F.col("is_device_percent_rank_count_2h").asc_nulls_last())

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("created_at") >= "2025-07-01")
    .withColumn("is_device", F.when(F.col("k") == "device", 1).otherwise(0))
)

scored = (
    cleaned
    .filter(F.col("created_at") < "2023-05-02")
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "value",
        "is_device",
        F.percent_rank().over(w).alias("is_device_percent_rank"),
    )
)

scored_carts = (
    scored
    .select(
        "id",
        "coupon_codes",
        "value",
        F.sum("is_device_percent_rank").over(w2).alias("is_device_percent_rank_sum_2h"),
        F.count("is_device_percent_rank").over(w3).alias("is_device_percent_rank_count_2h"),
    )
)

filtered = (
    scored_carts
    .filter(F.col("is_device_percent_rank_count_2h") != 12)
    .select(
        "id",
        "coupon_codes",
        "value",
        "is_device_percent_rank_sum_2h",
        "is_device_percent_rank_count_2h",
        F.size("coupon_codes").alias("coupon_codes_count"),
        F.size("coupon_codes").alias("coupon_codes_count_2"),
    )
)

scored_carts_2 = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w4).alias("coupon_codes_count_dense_rank"),
        F.dense_rank().over(w5).alias("coupon_codes_count_2_dense_rank"),
        F.percent_rank().over(w_ordered).alias("coupon_codes_count_percent_rank"),
    )
)

scored_carts_3 = (
    scored_carts_2
    .select(
        "id",
        "coupon_codes",
        "coupon_codes_count",
        "coupon_codes_count_2",
        "coupon_codes_count_dense_rank",
        "coupon_codes_count_2_dense_rank",
        "coupon_codes_count_percent_rank",
        F.percent_rank().over(w7).alias("coupon_codes_count_percent_rank_percent_rank"),
        F.rank().over(w8).alias("coupon_codes_count_2_dense_rank_rank"),
        F.dense_rank().over(w9).alias("is_device_percent_rank_count_2h_dense_rank"),
    )
)

result = (
    scored_carts_3
    .groupBy(
        F.when(F.col("coupon_codes_count") >= 90, "LARGE").otherwise("SMALL").alias("coupon_codes_count_tier"),
        F.when(F.col("coupon_codes_count_percent_rank") >= 0.74, "HIGH").otherwise("LOW").alias("coupon_codes_count_percent_rank_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
