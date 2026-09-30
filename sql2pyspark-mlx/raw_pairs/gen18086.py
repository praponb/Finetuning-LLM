from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("promo")
w3 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    exploded
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "price",
        F.avg("product_id").over(w).alias("product_id_avg_2h"),
        F.round((F.col("product_id") / F.sum("product_id").over(w2)), 4).alias("product_id_share"),
        F.max("price").over(w3).alias("price_max_30m"),
    )
)
