from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "signup_date"), "customer_id")
w2 = Window.partitionBy("customer_id").orderBy("price")
w3 = Window.partitionBy("customer_id").orderBy(F.col("price").desc())

exploded = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

enriched = (
    exploded
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select("s.customer_id", "s.price", F.col("p.signup_date"))
)

kept_keys = (
    enriched
    .select("customer_id")
    .subtract(
        df_carts
        .filter(F.col("created_at") >= "2025-03-01")
        .select("customer_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "customer_id",
        "signup_date",
        F.min("price").over(w).alias("signup_date_period_min_price"),
        F.dense_rank().over(w2).alias("price_dense_rank"),
        F.dense_rank().over(w3).alias("price_dense_rank_2"),
    )
)

result = metrics.select("customer_id", "signup_date", "price_dense_rank_2")
