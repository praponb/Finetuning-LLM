from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("pos").desc())
w2 = Window.orderBy(F.col("pos").desc(), "id", "pos")
w3 = Window.orderBy("created_at", "id", "pos").rowsBetween(-6, -1)
w4 = Window.orderBy("created_at", "id", "pos")
w5 = Window.partitionBy("promo").orderBy("qty", "id", "pos")

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .withColumn("pos_percent_rank", F.percent_rank().over(w))
    .withColumn("pos_bucket", F.ntile(4).over(w2))
    .withColumn("pos_avg_6_prior", F.avg("pos").over(w3))
    .withColumn("prev_pos", F.lag("pos", 1).over(w4))
    .withColumn("next_qty", F.lead("qty", 2).over(w5))
)
