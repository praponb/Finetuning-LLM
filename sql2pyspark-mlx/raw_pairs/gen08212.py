from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy(F.col("qty").desc_nulls_last())
w2 = Window.partitionBy("value", "customer_id").orderBy(F.col("price").desc_nulls_first())
w3 = Window.partitionBy(F.year("created_at"), "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("price_dense_rank_tier").orderBy(F.col("avg_price_dense_rank_whole").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

flattened = (
    lines
    .select(
        "customer_id",
        "created_at",
        "qty",
        "price",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

joined = (
    flattened
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.age"), F.col("p.signup_date"), F.col("p.credit_limit"))
)

metrics = (
    joined
    .select(
        "customer_id",
        "created_at",
        F.cume_dist().over(w).alias("qty_cume_dist"),
        F.dense_rank().over(w2).alias("price_dense_rank"),
        F.max("age").over(w3).alias("age_max_1h"),
    )
)

joined_carts = (
    metrics
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.customer_id",
        "e.created_at",
        "e.price_dense_rank",
        F.col("c.signup_date"),
        F.col("c.name"),
    )
)

cleaned = (
    joined_carts
    .filter(F.year("signup_date") >= 2023)
    .select(
        "customer_id",
        "price_dense_rank",
        "signup_date",
        "name",
        F.add_months("signup_date", 1).alias("signup_date_plus_months"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("customer_id").isNotNull() & (F.col("signup_date_plus_months") > "2024-02-17"))
    .withColumn("price_dense_rank_whole", F.ceil("price_dense_rank"))
)

daily = (
    cleaned_carts
    .groupBy(
        F.when(F.col("price_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("price_dense_rank_tier"),
        "customer_id",
    )
    .agg(
        F.percentile_approx("price_dense_rank_whole", 0.95).alias("price_dense_rank_whole_p95"),
        F.percentile_approx("price_dense_rank", 0.5).alias("price_dense_rank_p50"),
        F.round(F.avg("price_dense_rank_whole"), 2).alias("avg_price_dense_rank_whole"),
        F.min("price_dense_rank_whole").alias("min_price_dense_rank_whole"),
        F.percentile_approx("price_dense_rank", 0.5).alias("price_dense_rank_p50_2"),
    )
    .withColumn("avg_price_dense_rank_whole_dense_rank", F.dense_rank().over(w4))
)

result = (
    daily
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "price_dense_rank_tier",
        "customer_id",
        "price_dense_rank_whole_p95",
        "price_dense_rank_p50",
        "avg_price_dense_rank_whole",
        "min_price_dense_rank_whole",
        "price_dense_rank_p50_2",
        F.col("max_account_balance"),
    )
)
