from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy(F.month("created_at"), "value")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("customer_id", "value")
w_recent_2 = Window.partitionBy(F.date_trunc("month", "created_at"), "value").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w7 = Window.partitionBy(F.month("created_at"), "customer_id").orderBy(F.col("qty_count_2h").desc())
w8 = Window.orderBy(F.col("customer_id_count_qty").desc())
w9 = Window.partitionBy("customer_id").orderBy(F.col("customer_id_count_qty").desc_nulls_first())

flattened = (
    df_carts
    .withColumn("entry", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("entry.product_id"),
        F.col("entry.qty"),
    )
)

flattened_carts = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "qty",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

scored = (
    flattened_carts
    .select(
        "customer_id",
        "created_at",
        "qty",
        F.stddev_samp("qty").over(w_recent).alias("created_at_month_stddev_samp_qty"),
        F.count("qty").over(w2).alias("customer_id_count_qty"),
        F.count("qty").over(w3).alias("qty_count_2h"),
        F.sum("qty").over(w4).alias("customer_id_sum_qty"),
        F.avg("qty").over(w_recent_2).alias("qty_avg_1h"),
    )
)

enriched = (
    scored
    .filter(F.col("qty_avg_1h") > 3)
    .select(
        "created_at",
        "created_at_month_stddev_samp_qty",
        "customer_id_count_qty",
        "qty_count_2h",
        F.count("customer_id_sum_qty").over(w6).alias("customer_id_sum_qty_count_15m"),
        F.percent_rank().over(w7).alias("qty_count_2h_percent_rank"),
        F.cume_dist().over(w8).alias("customer_id_count_qty_cume_dist"),
        F.dense_rank().over(w9).alias("customer_id_count_qty_dense_rank"),
    )
)

result = (
    enriched
    .select(
        "created_at",
        "created_at_month_stddev_samp_qty",
        "customer_id_count_qty",
        "qty_count_2h",
        "customer_id_sum_qty_count_15m",
        "qty_count_2h_percent_rank",
        "customer_id_count_qty_cume_dist",
        "customer_id_count_qty_dense_rank",
    )
    .orderBy(F.col("qty_count_2h_percent_rank").desc(), F.col("created_at").desc())
)
