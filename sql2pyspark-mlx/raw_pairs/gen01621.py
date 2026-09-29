from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy(F.year("created_at"), "promo")
w2 = Window.partitionBy("customer_id").orderBy(F.col("product_id").desc_nulls_first())
w3 = Window.partitionBy("promo", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

metrics = (
    lines
    .withColumn("created_at_year_sum_product_id", F.sum("product_id").over(w_running))
    .withColumn("product_id_dense_rank", F.dense_rank().over(w2))
    .withColumn("qty_avg_15m", F.avg("qty").over(w3))
)

filtered = (
    metrics
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "qty",
        "price",
        "promo",
        "created_at_year_sum_product_id",
        "product_id_dense_rank",
        "qty_avg_15m",
        F.greatest("product_id_dense_rank", "qty").alias("greatest_product_id_dense_rank_qty"),
        F.coalesce("promo", F.lit("OTHER")).alias("promo_clean"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.credit_limit"), F.col("b.signup_date"), F.col("b.country"))
)

kept_keys = (
    enriched
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("created_at") >= "2023-05-01")
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

pivoted = (
    kept_rows
    .groupBy("promo", "customer_id")
    .pivot("country", ["DE", "UK"])
    .agg(F.avg("created_at_year_sum_product_id").alias("value_sum"))
)

result = (
    pivoted
    .select("promo", "customer_id", "DE", "UK")
    .orderBy(F.col("DE").desc())
)
