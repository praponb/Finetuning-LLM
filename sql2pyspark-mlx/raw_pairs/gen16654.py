from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_first())
w3 = Window.partitionBy("promo").orderBy(F.col("product_id").desc())

flattened = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

base = (
    flattened
    .filter(~F.col("promo").eqNullSafe(F.lit("WELCOME5")))
    .select(
        "coupon_codes",
        "product_id",
        "price",
        "promo",
        F.array_contains("coupon_codes", "BUNDLE").alias("is_bundle"),
    )
)

scored = (
    base
    .filter(F.size("coupon_codes") >= 1)
    .select(
        "price",
        "promo",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.max("price").over(w).alias("promo_max_price"),
        F.percent_rank().over(w2).alias("product_id_percent_rank"),
        F.dense_rank().over(w3).alias("product_id_dense_rank"),
    )
)

cleaned = (
    scored
    .filter(F.col("product_id_dense_rank") <= 5)
    .select(
        "*",
        F.when(F.col("promo_max_price") >= 270, "High").when(F.col("promo_max_price") >= 74, "Medium").otherwise("Low").alias("promo_max_price_level"),
    )
)

result = (
    cleaned
    .select("promo", "product_id_percent_rank", "product_id_share")
    .unpivot(
        ids=["promo"],
        values=["product_id_percent_rank", "product_id_share"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
