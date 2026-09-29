from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("product_id").desc())
w_part = Window.partitionBy(F.date_trunc("WEEK", "created_at"), "promo")

lines = (
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
    lines
    .select(
        "id",
        "created_at",
        "promo",
        F.cume_dist().over(w).alias("product_id_cume_dist"),
        F.count("product_id").over(w_part).alias("created_at_period_count_product_id"),
    )
)
