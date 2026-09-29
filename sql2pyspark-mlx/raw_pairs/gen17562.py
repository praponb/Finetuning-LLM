from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc_nulls_last())
w2 = Window.orderBy(F.col("product_id").desc_nulls_last())
w_running = Window.partitionBy("country")
w4 = Window.partitionBy("segment", "city")
w_hist = Window.partitionBy("city").orderBy("age")
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("product_id_percent_rank").desc())

flattened = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
    )
)

joined = (
    flattened
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.coupon_codes",
        "a.product_id",
        "a.qty",
        F.col("d.country"),
    )
)

with_windows = (
    joined
    .filter(F.col("customer_id").isin([1, 3]))
    .select(
        "id",
        "customer_id",
        F.rank().over(w).alias("qty_rank"),
        F.percent_rank().over(w2).alias("product_id_percent_rank"),
        F.avg("product_id").over(w_running).alias("country_avg_product_id"),
    )
)

joined_carts = (
    with_windows
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.signup_date"), F.col("b.segment"), F.col("b.city"))
)

with_dims = (
    joined_carts
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.city").alias("customer_city"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.product_id_percent_rank",
        "a.country_avg_product_id",
        "a.segment",
        "a.city",
        F.col("p.age"),
        F.col("p.credit_limit"),
    )
)

with_windows_carts = (
    enriched
    .select(
        "*",
        F.stddev_samp("product_id_percent_rank").over(w4).alias("segment_stddev_samp_product_id_percent_rank"),
        F.rank().over(w_hist).alias("age_rank"),
        F.cume_dist().over(w_ordered).alias("product_id_percent_rank_cume_dist"),
    )
)

summary = (
    with_windows_carts
    .groupBy(
        F.when(F.col("country_avg_product_id") >= 11, "HIGH").otherwise("LOW").alias("country_avg_product_id_tier"),
    )
    .agg(
        F.max("age").alias("max_age"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.round(F.sum("country_avg_product_id") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("country_avg_product_id_per_berlin"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("credit_limit")).otherwise(0)).alias("bangkok_credit_limit"),
    )
    .filter(F.col("max_age") > 0)
)

result = (
    summary
    .select(
        "country_avg_product_id_tier",
        "corporate_rows",
        "country_avg_product_id_per_berlin",
        "bangkok_credit_limit",
    )
    .orderBy(F.col("corporate_rows").desc())
)
