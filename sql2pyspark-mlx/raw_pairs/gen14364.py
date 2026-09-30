from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("category").orderBy(F.col("cost").desc_nulls_first())
w2 = Window.partitionBy("brand")
w_hist = Window.partitionBy("brand", "category")
w_running = Window.orderBy(F.col("brand_avg_price_rounded").desc_nulls_first())
w5 = Window.partitionBy("category").orderBy(F.col("brand_sum_price").desc_nulls_last())
w_recent = Window.partitionBy("category").orderBy(F.col("brand_min_cost_percent_rank").desc())

filtered = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-08-07")))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        F.length("product_name").alias("product_name_length"),
    )
)

matched_keys = (
    filtered
    .select("id")
    .intersectAll(
        df_products
        .filter(F.col("launch_date") > "2024-01-01")
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = verified.select("*", F.percent_rank().over(w_part).alias("cost_percent_rank"))

cleaned = (
    enriched
    .filter(~F.col("brand").eqNullSafe(F.lit("Acme")))
    .withColumn("price_rounded", F.round("price", 1))
)

enriched_products = (
    cleaned
    .filter(F.col("category").isin(["Electronics", "Home", "Grocery"]))
    .select(
        "id",
        "category",
        "product_name_length",
        "price_rounded",
        F.min("cost_percent_rank").over(w2).alias("brand_min_cost_percent_rank"),
        F.avg("price_rounded").over(w_hist).alias("brand_avg_price_rounded"),
        F.sum("price").over(w_hist).alias("brand_sum_price"),
    )
)

with_windows = (
    enriched_products
    .select(
        "id",
        "category",
        "product_name_length",
        "price_rounded",
        "brand_avg_price_rounded",
        "brand_sum_price",
        F.cume_dist().over(w_running).alias("brand_avg_price_rounded_cume_dist"),
        F.percent_rank().over(w5).alias("brand_sum_price_percent_rank"),
        F.rank().over(w_running).alias("brand_avg_price_rounded_rank"),
        F.rank().over(w_recent).alias("brand_min_cost_percent_rank_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "category",
        "price_rounded",
        "brand_avg_price_rounded_cume_dist",
        "brand_sum_price_percent_rank",
        "brand_avg_price_rounded_rank",
        "brand_min_cost_percent_rank_rank",
        F.when(F.col("category").rlike("^(Electronics|Grocery)$"), "EXTENDED").otherwise("GROUP_A").alias("category_zone"),
    )
    .orderBy(F.col("category").desc(), "brand_sum_price_percent_rank")
)
