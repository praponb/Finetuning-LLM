from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("price_p90").desc())

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "rec"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("line_no"),
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
    )
)

result = (
    exploded
    .groupBy("product_id", "line_no")
    .agg(
        F.percentile_approx("price", 0.9).alias("price_p90"),
        F.percentile_approx("product_id", 0.9).alias("product_id_p90"),
        F.min("created_at").alias("first_seen"),
    )
    .withColumn("price_p90_dense_rank", F.dense_rank().over(w))
)
