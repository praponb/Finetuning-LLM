from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc_nulls_first())
w2 = Window.orderBy(F.col("product_id").desc_nulls_first())

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

result = (
    exploded
    .filter(F.col("price") <= 130)
    .select(
        "id",
        "created_at",
        "product_id",
        F.rank().over(w).alias("qty_rank"),
        F.percent_rank().over(w2).alias("product_id_percent_rank"),
    )
)
