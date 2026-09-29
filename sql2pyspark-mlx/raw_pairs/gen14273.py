from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_hist = Window.partitionBy("customer_id")
w3 = Window.partitionBy("element")
w_grp = Window.partitionBy(F.to_date("created_at"), "customer_id").orderBy("qty")
w5 = Window.partitionBy(F.to_date("created_at"), "city")
w6 = Window.partitionBy("element", "city")
w7 = Window.partitionBy("element").orderBy(F.col("signup_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("element", "city").orderBy(F.col("product_id_share").desc())

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)

exploded_carts = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "product_id",
        "qty",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

joined = (
    exploded_carts
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.segment").isin(["Corporate", "SMB", "Consumer"]))
    .select("e.*", F.col("b.signup_date"), F.col("b.name"), F.col("b.city"))
)

enriched = (
    joined
    .withColumn("qty_avg_1h", F.avg("qty").over(w))
    .withColumn("customer_id_min_product_id", F.min("product_id").over(w_hist))
    .withColumn("product_id_share", F.round((F.col("product_id") / F.sum("product_id").over(w3)), 4))
)

filtered = (
    enriched
    .filter(F.col("customer_id_min_product_id") > 9)
    .withColumn("signup_date_period", F.date_trunc("WEEK", "signup_date"))
    .withColumn("resolved_element", F.coalesce("element", F.lit("DIRECT")))
)

with_windows = (
    filtered
    .select(
        "id",
        "customer_id",
        "product_id",
        "element",
        "signup_date",
        "city",
        "product_id_share",
        "resolved_element",
        F.dense_rank().over(w_grp).alias("qty_dense_rank"),
        F.stddev_samp("product_id").over(w5).alias("created_at_to_date_stddev_samp_product_id"),
        F.sum("qty").over(w6).alias("element_sum_qty"),
        F.count("product_id_share").over(w7).alias("product_id_share_count_90d"),
        F.cume_dist().over(w8).alias("product_id_share_cume_dist"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("city").isNotNull() & (F.col("city") == "Bangkok"))
    .withColumn("product_id_share_cume_dist_whole", F.floor("product_id_share_cume_dist"))
    .withColumn("product_id_share_per_product_id", F.round((F.col("product_id_share") / F.col("product_id")), 2))
    .withColumn("element_clean", F.coalesce("element", F.lit("DIRECT")))
)

result = (
    cleaned
    .select(
        "id",
        "customer_id",
        "product_id",
        "element",
        "product_id_share",
        "resolved_element",
        "qty_dense_rank",
        "created_at_to_date_stddev_samp_product_id",
        "product_id_share_count_90d",
        F.when(F.col("element").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_B").when(F.col("element").rlike("^(SPRING10|VIP)$"), "EXTENDED").otherwise("GROUP_A").alias("element_zone"),
    )
)
