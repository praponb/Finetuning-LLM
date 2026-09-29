from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country").orderBy(F.col("product_id").desc())
w3 = Window.partitionBy("promo")

flattened = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.promo"),
    )
)

with_dims = (
    flattened
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.age"), F.col("b.country"), F.col("b.segment"))
)

filtered = (
    with_dims
    .filter(F.col("created_at") >= F.lit("2025-03-01 22:00:00").cast("timestamp"))
    .withColumn("promo_filled", F.coalesce("promo", F.lit("N/A")))
    .withColumn("segment_clean", F.coalesce("segment", F.lit("OTHER")))
)

base = (
    filtered
    .filter(F.col("promo").isNotNull())
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id",
        "promo",
        "age",
        "country",
        "segment",
        "promo_filled",
        (F.col("age") + F.col("product_id")).alias("age_plus_product_id"),
    )
)

result = (
    base
    .filter(F.col("age").between(35, 47))
    .withColumn("country_stddev_samp_age_plus_product_id", F.stddev_samp("age_plus_product_id").over(w))
    .withColumn("product_id_percent_rank", F.percent_rank().over(w2))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w3)), 4))
)
