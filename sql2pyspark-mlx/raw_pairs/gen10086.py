from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("price").desc(), "id", "pos")
w2 = Window.partitionBy("promo").orderBy(F.col("price").desc_nulls_last())
w3 = Window.partitionBy("promo").orderBy(F.col("price_dense_rank").desc_nulls_last(), "id", "pos")
w4 = Window.orderBy("price", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("price_dense_rank", "id", "pos")
w_part = Window.orderBy(F.col("price_dense_rank").desc_nulls_last())

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "coupon_codes",
        "pos",
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

metrics = (
    flattened
    .select(
        "*",
        F.ntile(10).over(w).alias("price_bucket"),
        F.dense_rank().over(w2).alias("price_dense_rank"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "pos", "qty", "price", "price_dense_rank")
)

scored = (
    latest
    .select(
        "id",
        "pos",
        "price",
        "price_dense_rank",
        F.sum("qty").over(w4).alias("running_qty"),
        F.lead("price", 1, 0).over(w_running).alias("next_price"),
        F.percent_rank().over(w_part).alias("price_dense_rank_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("next_price") >= 246, "LARGE").otherwise("SMALL").alias("next_price_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("price_dense_rank"), 2).alias("avg_price_dense_rank"),
        F.percentile_approx("price", 0.9).alias("price_p90"),
        F.percentile_approx("pos", 0.95).alias("pos_p95"),
        F.percentile_approx("price_dense_rank", 0.9).alias("price_dense_rank_p90"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("next_price_tier").asc_nulls_last())
)
