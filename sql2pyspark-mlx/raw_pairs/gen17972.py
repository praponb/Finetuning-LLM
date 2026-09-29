from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id", "promo").orderBy("price", "id", "pos").rowsBetween(-6, Window.currentRow)

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    exploded
    .select(
        "id",
        "created_at",
        "pos",
        "qty",
        "promo",
        F.max("price").over(w_part).alias("last7_max_price"),
        F.min("price").over(w_part).alias("last7_min_price"),
        F.avg("price").over(w_part).alias("last7_avg_price"),
        F.count("price").over(w_part).alias("last7_count_price"),
    )
)
