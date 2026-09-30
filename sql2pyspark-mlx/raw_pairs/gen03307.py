from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("pos").desc_nulls_last())
w2 = Window.orderBy(F.col("pos").asc_nulls_last())
w3 = Window.partitionBy("value")
w_ordered = Window.partitionBy("value").orderBy(F.col("pos").desc_nulls_last())
w5 = Window.partitionBy("value").orderBy(F.col("pos").desc_nulls_first())

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("line.qty"),
        F.col("line.price"),
    )
)

lines = flattened.select("id", "pos", F.explode("coupon_codes").alias("value"))

cleaned = (
    lines
    .filter(~F.col("value").eqNullSafe(F.lit("WELCOME5")))
    .withColumn("value_title", F.initcap(F.trim("value")))
)

scored = (
    cleaned
    .filter(F.col("pos") >= 3)
    .withColumn("pos_cume_dist", F.cume_dist().over(w))
    .withColumn("pos_rank", F.rank().over(w2))
    .withColumn("value_max_pos", F.max("pos").over(w3))
    .withColumn("pos_rank_2", F.rank().over(w_ordered))
    .withColumn("pos_cume_dist_2", F.cume_dist().over(w5))
)

result = scored.select("id", "pos", "value", "value_title", "pos_rank", "pos_cume_dist_2")
