from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("promo").orderBy("qty")
w2 = Window.partitionBy("promo")
w3 = Window.orderBy(F.col("qty").desc_nulls_last())

exploded = df_carts.select("id", "coupon_codes", F.inline("items"))

result = (
    exploded
    .filter(F.col("promo").isin(["WELCOME5", "SPRING10", "BUNDLE"]))
    .select(
        "price",
        "promo",
        F.dense_rank().over(w_part).alias("qty_dense_rank"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
        F.percent_rank().over(w3).alias("qty_percent_rank"),
    )
)
