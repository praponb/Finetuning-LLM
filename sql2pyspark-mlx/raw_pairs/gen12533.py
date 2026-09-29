from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc_nulls_first())
w2 = Window.orderBy(F.col("product_id").asc_nulls_first())

lines = df_carts.select("id", F.inline("items"))

result = (
    lines
    .filter(F.col("promo") != "BUNDLE")
    .select(
        "id",
        "product_id",
        "qty",
        "price",
        "promo",
        F.rank().over(w).alias("qty_rank"),
        F.dense_rank().over(w2).alias("product_id_dense_rank"),
    )
)
