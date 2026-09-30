from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("product_id")
w2 = Window.orderBy(F.col("product_id").desc_nulls_last())

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        (F.col("pos") + 1).alias("item_no"),
        F.col("elem.product_id"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

lines_carts = (
    lines
    .select(
        "id",
        "customer_id",
        "item_no",
        "product_id",
        "promo",
        F.explode("coupon_codes").alias("value"),
    )
)

with_dims = (
    lines_carts
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.country") == "JP")
    .select("a.customer_id", "a.item_no", "a.product_id", F.col("p.email"))
)

scored = (
    with_dims
    .select(
        "customer_id",
        "item_no",
        F.cume_dist().over(w).alias("product_id_cume_dist"),
        F.dense_rank().over(w2).alias("product_id_dense_rank"),
    )
)

joined = (
    scored
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select(
        "src.customer_id",
        "src.item_no",
        F.col("b.country"),
        F.col("b.credit_limit"),
        F.col("b.city"),
    )
)

result = (
    joined
    .groupBy(
        "customer_id",
        F.when(F.col("item_no") >= 3, "PREMIUM").otherwise("STANDARD").alias("item_no_tier"),
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.percentile_approx("item_no", 0.9).alias("item_no_p90"),
    )
    .orderBy("customer_id")
)
