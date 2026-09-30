from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("promo").orderBy(F.col("qty").desc())

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    lines
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.avg("qty").over(w).alias("qty_avg_15m"),
        F.dense_rank().over(w2).alias("qty_dense_rank"),
    )
)
