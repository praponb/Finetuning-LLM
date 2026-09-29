from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("created_at"), "promo").orderBy(F.col("qty").desc_nulls_last())
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.orderBy(F.col("qty").desc_nulls_first())
w_ordered = Window.orderBy(F.col("qty_cume_dist").desc_nulls_first())
w6 = Window.partitionBy("promo")

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

with_windows = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "price",
        "promo",
        F.cume_dist().over(w).alias("qty_cume_dist"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.max("price").over(w3).alias("price_max_1h"),
        F.round((F.col("product_id") / F.sum("product_id").over(w2)), 4).alias("product_id_share"),
        F.dense_rank().over(w4).alias("qty_dense_rank"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select(
        "src.customer_id",
        "src.created_at",
        "src.price",
        "src.promo",
        "src.qty_cume_dist",
        "src.price_share",
        "src.price_max_1h",
        F.col("c.signup_date"),
        F.col("c.city"),
    )
)

with_windows_carts = (
    joined
    .filter(F.col("price") > 200)
    .select(
        "*",
        F.dense_rank().over(w_ordered).alias("qty_cume_dist_dense_rank"),
        F.max("price_share").over(w6).alias("promo_max_price_share"),
    )
)

enriched = (
    with_windows_carts
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.credit_limit"),
        F.col("d.city").alias("customer_city"),
        F.col("d.age"),
    )
)

result = (
    enriched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.col("c.country").alias("country"),
        F.col("c.city").alias("customer_city"),
        F.col("e.promo").alias("promo"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.price_share")).alias("total_price_share"),
    )
    .orderBy(F.col("customer_city").desc())
)
