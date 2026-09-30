from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("product_id").desc_nulls_first())
w_ordered = Window.orderBy(F.col("price").desc())
w3 = Window.orderBy(F.col("greatest_price_percent_rank_price_times_price_percent_rank").desc_nulls_first())
w4 = Window.orderBy(F.col("product_id_rank").desc_nulls_last())
w5 = Window.orderBy(F.col("product_id_rank").desc())
w6 = Window.orderBy(F.col("greatest_price_percent_rank_price_times_price_percent_rank").desc_nulls_last())
w7 = Window.orderBy(F.col("product_id_rank").desc_nulls_first())

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

scored = (
    exploded
    .filter(F.col("product_id") != 4)
    .withColumn("product_id_rank", F.rank().over(w))
    .withColumn("price_percent_rank", F.percent_rank().over(w_ordered))
)

filtered = (
    scored
    .filter(F.col("qty").between(3, 4))
    .withColumn("greatest_price_percent_rank_price", F.greatest("price_percent_rank", "price"))
)

valid = (
    filtered
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .withColumn("greatest_price_percent_rank_price_times_price_percent_rank", (F.col("greatest_price_percent_rank_price") * F.col("price_percent_rank")))
    .withColumn("sorted_coupon_codes", F.sort_array("coupon_codes"))
)

result = (
    valid
    .select(
        "id",
        "coupon_codes",
        "product_id",
        "qty",
        "price",
        "product_id_rank",
        "price_percent_rank",
        "greatest_price_percent_rank_price_times_price_percent_rank",
        "sorted_coupon_codes",
        F.dense_rank().over(w3).alias("greatest_price_percent_rank_price_times_price_percent_rank_dense_rank"),
        F.rank().over(w4).alias("product_id_rank_rank"),
        F.dense_rank().over(w5).alias("product_id_rank_dense_rank"),
        F.dense_rank().over(w6).alias("greatest_price_percent_rank_price_times_price_percent_rank_dense_rank_2"),
        F.percent_rank().over(w7).alias("product_id_rank_percent_rank"),
    )
)
