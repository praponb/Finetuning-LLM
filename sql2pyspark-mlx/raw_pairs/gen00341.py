from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("qty").desc())

lines = (
    df_carts
    .withColumn("entry", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("entry.qty"),
        F.col("entry.price"),
    )
)

result = (
    lines
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.avg("price").over(w2).alias("price_avg_30m"),
        F.dense_rank().over(w3).alias("qty_dense_rank"),
    )
)
