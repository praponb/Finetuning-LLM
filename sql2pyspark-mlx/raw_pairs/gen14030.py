from pyspark.sql import functions as F

result = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "created_at",
        (F.col("pos") + 1).alias("line_no"),
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)
