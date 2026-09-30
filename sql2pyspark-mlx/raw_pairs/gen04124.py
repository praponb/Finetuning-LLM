from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy("price")
w_part = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    exploded
    .filter(F.col("created_at") >= "2024-04-01")
    .select(
        "product_id",
        "promo",
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.cume_dist().over(w2).alias("price_cume_dist"),
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.max("price").over(w).alias("promo_max_price"),
        F.max("price").over(w_part).alias("price_max_30m"),
    )
)
