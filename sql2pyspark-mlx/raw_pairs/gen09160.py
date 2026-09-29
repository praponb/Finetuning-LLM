from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").asc_nulls_first())
w2 = Window.orderBy(F.col("stock_qty").desc_nulls_first())
w3 = Window.orderBy(F.col("cost_rank").desc_nulls_first())
w4 = Window.orderBy(F.col("cost_rank").asc_nulls_first())
w5 = Window.orderBy(F.col("stock_qty").desc_nulls_last())

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-06-21").cast("date"))
    .select(
        "id",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.col("stock_qty").cast("string").alias("stock_qty_num"),
        F.when(F.col("price") >= 1010, "High").when(F.col("price") >= 400, "Medium").otherwise("Low").alias("price_tier"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

matched_keys = (
    cleaned
    .select("id")
    .exceptAll(
        df_products
        .filter(F.col("price") >= 890)
        .select("id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .filter(F.col("price").isNotNull())
    .select(
        "price",
        "cost",
        "stock_qty",
        "stock_qty_num",
        "price_tier",
        F.rank().over(w).alias("cost_rank"),
    )
)

base = (
    enriched
    .filter(F.col("cost_rank") > 8)
    .select("*", F.round("price", 0).alias("price_rounded"))
)

enriched_products = (
    base
    .withColumn("stock_qty_dense_rank", F.dense_rank().over(w2))
    .withColumn("cost_rank_cume_dist", F.cume_dist().over(w3))
    .withColumn("cost_rank_percent_rank", F.percent_rank().over(w4))
    .withColumn("stock_qty_percent_rank", F.percent_rank().over(w5))
)

result = (
    enriched_products
    .select(
        "price",
        "stock_qty",
        "stock_qty_num",
        "cost_rank",
        "stock_qty_dense_rank",
        "cost_rank_percent_rank",
    )
    .orderBy(F.col("stock_qty_dense_rank").desc())
)
