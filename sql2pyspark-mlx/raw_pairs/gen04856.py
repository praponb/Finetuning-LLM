from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc_nulls_last())
w2 = Window.partitionBy("tag")
w3 = Window.partitionBy("promo").orderBy(F.col("tag_max_qty").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

lines = (
    exploded
    .select(
        "id",
        "customer_id",
        "qty",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

filtered = (
    lines
    .filter(F.col("promo").isNotNull() & (F.col("price") <= 150))
    .select("*", F.concat_ws("-", "tag", "promo").alias("tag_promo_key"))
)

metrics = (
    filtered
    .select(
        "id",
        "customer_id",
        "qty",
        "promo",
        "tag",
        "tag_promo_key",
        F.cume_dist().over(w).alias("price_cume_dist"),
        F.max("qty").over(w2).alias("tag_max_qty"),
    )
)

metrics_carts = (
    metrics
    .withColumn("tag_max_qty_percent_rank", F.percent_rank().over(w3))
    .withColumn("tag_min_tag_max_qty", F.min("tag_max_qty").over(w2))
)

with_dims = (
    metrics_carts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select("s.*", F.col("p.signup_date"), F.col("p.email"))
)

result = (
    with_dims
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "qty",
        "promo",
        "tag",
        "tag_promo_key",
        "tag_min_tag_max_qty",
        "signup_date",
        "email",
        F.when(F.col("promo").rlike("^(SPRING10|VIP)$"), "CORE").when(F.col("promo").rlike("^(BUNDLE|WELCOME5)$"), "EXTENDED").otherwise("PRIORITY").alias("promo_zone"),
        F.col("avg_account_balance"),
    )
)
