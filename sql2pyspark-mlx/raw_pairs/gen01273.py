from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").asc_nulls_last())
w2 = Window.orderBy(F.col("price").desc_nulls_first())
w3 = Window.orderBy(F.col("qty").desc_nulls_last())
w4 = Window.orderBy(F.col("price_cume_dist").asc_nulls_first())
w5 = Window.orderBy(F.col("price_rank").desc())
w6 = Window.orderBy(F.col("product_id").asc_nulls_last())

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
    )
)

kept_keys = (
    lines
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.col("customer_id") == 29)
        .select("id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.rank().over(w).alias("price_rank"),
        F.cume_dist().over(w2).alias("price_cume_dist"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w3).alias("qty_dense_rank"),
        F.rank().over(w4).alias("price_cume_dist_rank"),
        F.dense_rank().over(w5).alias("price_rank_dense_rank"),
    )
)

scored_carts = (
    scored
    .select(
        "product_id",
        "qty",
        "price",
        "qty_dense_rank",
        "price_cume_dist_rank",
        "price_rank_dense_rank",
        F.cume_dist().over(w).alias("price_cume_dist_2"),
        F.percent_rank().over(w6).alias("product_id_percent_rank"),
        F.cume_dist().over(w2).alias("price_cume_dist_3"),
    )
)

result = (
    scored_carts
    .groupBy(F.when(F.col("qty") >= 5, "LARGE").otherwise("SMALL").alias("qty_tier"))
    .agg(
        F.min("price").alias("min_price"),
        F.count("*").alias("row_count"),
        F.percentile_approx("price_cume_dist_2", 0.5).alias("price_cume_dist_2_p50"),
    )
    .filter(F.col("price_cume_dist_2_p50") < 1000000)
    .orderBy("price_cume_dist_2_p50")
)
