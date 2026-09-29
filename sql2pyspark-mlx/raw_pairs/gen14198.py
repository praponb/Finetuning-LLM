from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("months_since_created_at").desc_nulls_last())

exploded = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

exploded_carts = (
    exploded
    .select(
        "id",
        "created_at",
        "product_id",
        "qty",
        "promo",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

remaining_keys = (
    exploded_carts
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("created_at") < "2024-11-01")
        .select("id")
    )
)

kept_rows = (
    exploded_carts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("product_id").between(6, 7))
    .select(
        "id",
        "created_at",
        "qty",
        "promo",
        "code",
        (F.datediff(F.current_date(), "created_at") / 30).alias("months_since_created_at"),
        (F.col("qty") + F.col("product_id")).alias("qty_plus_product_id"),
    )
)

result = (
    valid
    .select(
        "id",
        "created_at",
        "qty",
        "promo",
        "code",
        F.count("qty_plus_product_id").over(w).alias("promo_count_qty_plus_product_id"),
        F.cume_dist().over(w2).alias("months_since_created_at_cume_dist"),
    )
)
