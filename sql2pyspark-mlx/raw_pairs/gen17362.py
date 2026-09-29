from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "country")
w_ordered = Window.partitionBy("country").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("promo").orderBy(F.col("qty_share_2").desc_nulls_first())

flattened = (
    df_carts
    .withColumn("rec", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.promo"),
    )
)

with_dims = (
    flattened
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.created_at",
        "a.product_id",
        "a.qty",
        "a.promo",
        F.col("p.country"),
        F.col("p.email"),
        F.col("p.name"),
    )
)

matched_keys = (
    with_dims
    .select("id", "customer_id")
    .subtract(
        df_carts
        .filter(F.to_date("created_at") >= "2024-04-01")
        .select("id", "customer_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "qty",
        "promo",
        "email",
        "name",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.sum("qty").over(w_ordered).alias("qty_sum_15m"),
        F.round((F.col("qty") / F.sum("qty").over(w3)), 4).alias("qty_share_2"),
    )
)

metrics_carts = (
    metrics
    .select(
        "name",
        "qty_share",
        "qty_sum_15m",
        "qty_share_2",
        F.cume_dist().over(w4).alias("qty_share_2_cume_dist"),
    )
)

cleaned = (
    metrics_carts
    .filter(F.col("name").isNull())
    .select(
        "qty_share",
        F.round((F.col("qty_share_2") * 1.07), 2).alias("qty_share_2_adjusted"),
        (F.col("qty_share") + F.col("qty_sum_15m")).alias("qty_share_plus_qty_sum_15m"),
    )
)

result = (
    cleaned
    .select("qty_share", "qty_share_2_adjusted", "qty_share_plus_qty_sum_15m")
    .orderBy(F.col("qty_share_2_adjusted").desc_nulls_first())
)
