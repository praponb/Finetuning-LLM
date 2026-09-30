from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "value")
w_part = Window.partitionBy("segment")
w3 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_first())
w4 = Window.partitionBy("value")
w5 = Window.orderBy(F.col("credit_limit_share").asc_nulls_last())
w_recent = Window.orderBy(F.col("name_length").asc_nulls_first())
w_hist = Window.orderBy(F.col("value_count_credit_limit").desc_nulls_first())
w8 = Window.orderBy(F.col("name_length").desc_nulls_last())

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("value"))
)

cleaned = (
    flattened
    .filter(F.col("created_at") >= F.lit("2025-03-02 01:00:00").cast("timestamp"))
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
)

joined = (
    cleaned
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.name").isNotNull())
    .select("s.*", F.col("c.email"))
)

joined_carts = (
    joined
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.value",
        "src.email",
        F.col("d.credit_limit"),
        F.col("d.segment"),
        F.col("d.name"),
    )
)

with_windows = (
    joined_carts
    .select(
        "id",
        "customer_id",
        "email",
        "name",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.stddev_samp("credit_limit").over(w_part).alias("segment_stddev_samp_credit_limit"),
        F.dense_rank().over(w3).alias("credit_limit_dense_rank"),
        F.count("credit_limit").over(w4).alias("value_count_credit_limit"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4).alias("credit_limit_share_2"),
    )
)

with_windows_carts = (
    with_windows
    .select(
        "id",
        "email",
        "name",
        "value_count_credit_limit",
        F.dense_rank().over(w5).alias("credit_limit_share_dense_rank"),
    )
)

cleaned_carts = (
    with_windows_carts
    .filter(F.col("value_count_credit_limit") > 15)
    .select(
        "*",
        F.length("name").alias("name_length"),
        F.substring("name", 1, 4).alias("name_prefix"),
    )
)

result = (
    cleaned_carts
    .select(
        "email",
        "name",
        "credit_limit_share_dense_rank",
        "name_length",
        F.cume_dist().over(w_recent).alias("name_length_cume_dist"),
        F.rank().over(w_hist).alias("value_count_credit_limit_rank"),
        F.rank().over(w8).alias("name_length_rank"),
    )
)
