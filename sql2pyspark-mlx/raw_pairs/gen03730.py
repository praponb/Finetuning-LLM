from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc())
w3 = Window.partitionBy("city").orderBy("last7_max_rating")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w5 = Window.orderBy("rating_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("product_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("product_price", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.orderBy("cumulative_rating_rank")
w9 = Window.orderBy("product_price", "id").rowsBetween(-3, -1)
w10 = Window.orderBy("cumulative_rating_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("rating_rank", "id")

scored = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.sum("rating").over(w).alias("last7_sum_rating"),
        F.max("rating").over(w).alias("last7_max_rating"),
        F.count("rating").over(w).alias("last7_count_rating"),
        F.rank().over(w2).alias("rating_rank"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest_match = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.last7_max_rating",
        "s.rating_rank",
        F.col("c.price").alias("product_price"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category") == "Fashion"), "left_semi")
)

with_windows = (
    not_flagged
    .filter(F.col("product_price") >= 1130)
    .withColumn("cumulative_rating_rank", F.sum("rating_rank").over(w5))
    .withColumn("running_rating_rank", F.sum("rating_rank").over(w_ordered))
    .withColumn("product_price_avg_6", F.avg("product_price").over(w7))
    .withColumn("running_product_price", F.sum("product_price").over(w_ordered))
)

scored_suppliers = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w8).alias("cumulative_rating_rank_cume_dist"),
        F.avg("product_price").over(w9).alias("product_price_avg_3_prior"),
        F.last("last7_max_rating", ignorenulls=True).over(w10).alias("last_known_last7_max_rating"),
        F.lead("rating_rank", 1).over(w11).alias("next_rating_rank"),
    )
)

result = (
    scored_suppliers
    .select(
        "id",
        "last7_max_rating",
        "rating_rank",
        "product_price",
        "cumulative_rating_rank",
        "running_rating_rank",
        "running_product_price",
        "cumulative_rating_rank_cume_dist",
        "product_price_avg_3_prior",
    )
)
