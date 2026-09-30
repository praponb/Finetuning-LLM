from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "created_at",
        "pos",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

result = (
    lines
    .withColumn("price_max_15m", F.max("price").over(w_running))
    .withColumn("pos_avg_15m", F.avg("pos").over(w_running))
)
