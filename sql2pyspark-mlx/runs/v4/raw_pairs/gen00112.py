from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy("qty")
w3 = Window.partitionBy("promo").orderBy(F.col("price").desc())

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    flattened
    .withColumn("promo_sum_price", F.sum("price").over(w))
    .withColumn("qty_percent_rank", F.percent_rank().over(w2))
    .withColumn("price_percent_rank", F.percent_rank().over(w3))
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
)
