from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy(F.year("created_at"), "customer_id").orderBy(F.col("qty").asc_nulls_last())
w3 = Window.partitionBy("promo").orderBy(F.col("price").asc_nulls_last())
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("promo", "customer_id")

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

with_windows = (
    flattened
    .select(
        "*",
        F.avg("product_id").over(w).alias("product_id_avg_1h"),
        F.rank().over(w2).alias("qty_rank"),
        F.percent_rank().over(w3).alias("price_percent_rank"),
        F.min("qty").over(w4).alias("customer_id_min_qty"),
    )
)

enriched = (
    with_windows
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .filter(F.col("c.country").isin(["TH", "US", "DE"]))
    .select(
        "e.id",
        "e.customer_id",
        "e.coupon_codes",
        "e.product_id",
        "e.qty",
        "e.price",
        "e.promo",
        "e.price_percent_rank",
        "e.customer_id_min_qty",
        F.col("c.city"),
    )
)

enriched_carts = (
    enriched
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w5)), 4))
)

totals = (
    enriched_carts
    .groupBy(
        F.when(F.col("price") >= 229, "HIGH").otherwise("LOW").alias("price_tier"),
        F.when(F.col("customer_id_min_qty") >= 4, "PREMIUM").otherwise("STANDARD").alias("customer_id_min_qty_tier"),
        "promo",
    )
    .agg(
        F.sum(F.when(F.col("promo") == "WELCOME5", F.col("qty")).otherwise(0)).alias("welcome5_qty"),
        F.round(F.avg("price_percent_rank"), 2).alias("avg_price_percent_rank"),
    )
    .filter(F.col("avg_price_percent_rank") > 0)
)

result = (
    totals
    .select("price_tier", "customer_id_min_qty_tier", "promo", "welcome5_qty")
    .orderBy(F.col("welcome5_qty").desc())
)
