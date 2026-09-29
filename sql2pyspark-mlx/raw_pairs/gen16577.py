from pyspark.sql import functions as F

result = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select(
        "id",
        "created_at",
        (F.col("pos") + 1).alias("position"),
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)
