from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("product_id").asc_nulls_first())
w_running = Window.partitionBy("element", "customer_id").orderBy(F.col("product_id").desc_nulls_first())
w3 = Window.partitionBy("city", "customer_id")
w4 = Window.orderBy(F.col("product_id_percent_rank").asc_nulls_first())
w5 = Window.orderBy(F.col("product_id_percent_rank").desc())
w6 = Window.partitionBy("customer_id").orderBy(F.col("qty").asc_nulls_last())
w7 = Window.orderBy(F.col("qty_share").desc())

lines = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

lines_carts = (
    lines
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "qty",
        "promo",
        F.explode("coupon_codes").alias("element"),
    )
)

scored = (
    lines_carts
    .select(
        "customer_id",
        "product_id",
        "qty",
        "promo",
        "element",
        F.cume_dist().over(w).alias("product_id_cume_dist"),
    )
)

joined = (
    scored
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.age") == 59)
    .select(
        "src.customer_id",
        "src.product_id",
        "src.qty",
        "src.element",
        F.col("b.city"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.percent_rank().over(w_running).alias("product_id_percent_rank"),
        F.round((F.col("qty") / F.sum("qty").over(w3)), 4).alias("qty_share"),
    )
)

joined_carts = (
    metrics
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.name"))
)

with_windows = (
    joined_carts
    .select(
        "*",
        F.dense_rank().over(w4).alias("product_id_percent_rank_dense_rank"),
        F.cume_dist().over(w5).alias("product_id_percent_rank_cume_dist"),
        F.cume_dist().over(w6).alias("qty_cume_dist"),
        F.count("qty").over(w3).alias("city_count_qty"),
        F.rank().over(w7).alias("qty_share_rank"),
    )
)

result = (
    with_windows
    .select(
        "product_id",
        "qty",
        "element",
        "city",
        "qty_share",
        "name",
        "product_id_percent_rank_dense_rank",
        "product_id_percent_rank_cume_dist",
        "qty_cume_dist",
    )
    .orderBy(
        F.col("product_id_percent_rank_dense_rank").asc_nulls_first(),
        F.col("city").desc(),
    )
)
