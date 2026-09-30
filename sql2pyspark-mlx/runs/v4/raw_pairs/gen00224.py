from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code").orderBy(F.col("qty").desc_nulls_first())
w2 = Window.partitionBy("code")
w3 = Window.orderBy(F.col("qty").asc_nulls_first())

exploded = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.qty"),
        F.col("item.price"),
    )
)

lines = exploded.select("qty", F.explode("coupon_codes").alias("code"))

enriched = (
    lines
    .select(
        "*",
        F.percent_rank().over(w).alias("qty_percent_rank"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
        F.cume_dist().over(w3).alias("qty_cume_dist"),
    )
)

result = enriched.select("qty", "qty_percent_rank", "qty_cume_dist")
