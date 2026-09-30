from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost_rounded").desc_nulls_first())
w2 = Window.partitionBy("brand", "category").orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("brand", "category")
w4 = Window.orderBy(F.col("cost_whole").asc_nulls_last())
w5 = Window.partitionBy("category").orderBy(F.col("cost_whole_percent_rank").asc_nulls_first())
w6 = Window.partitionBy("category").orderBy(F.col("brand_avg_cost_whole_percent_rank").desc_nulls_last())
w7 = Window.partitionBy("brand")
w8 = Window.partitionBy("brand").orderBy(F.col("cost_rounded_percent_rank").desc_nulls_last())
w9 = Window.partitionBy("brand", "category").orderBy(F.col("brand_avg_cost_whole_percent_rank").desc_nulls_first())

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-10-17").cast("date"))
    .select(
        "*",
        F.round("cost", 1).alias("cost_rounded"),
        (F.col("cost") / F.nullif("stock_qty", F.lit(0))).alias("cost_per_stock_qty"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

matched_keys = (
    cleaned
    .select("supplier_id", "id")
    .subtract(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("supplier_id", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-03-14"))) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "cost_rounded",
        "cost_per_stock_qty",
        "resolved_brand",
        F.ceil("cost").alias("cost_whole"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

metrics = (
    valid
    .withColumn("cost_rounded_percent_rank", F.percent_rank().over(w))
    .withColumn("cost_rank", F.rank().over(w2))
    .withColumn("corr_cost_price", F.corr("cost", "price").over(w3))
    .withColumn("cost_whole_percent_rank", F.percent_rank().over(w4))
)

eligible = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([1, 6, 7, 8])), "left_anti")
)

scored = (
    eligible
    .withColumn("brand_avg_cost_whole_percent_rank", F.avg("cost_whole_percent_rank").over(w3))
    .withColumn("cost_whole_percent_rank_percent_rank", F.percent_rank().over(w5))
)

enriched = (
    scored
    .select(
        "*",
        F.percent_rank().over(w6).alias("brand_avg_cost_whole_percent_rank_percent_rank"),
        F.max("cost_whole_percent_rank_percent_rank").over(w7).alias("brand_max_cost_whole_percent_rank_percent_rank"),
        F.cume_dist().over(w8).alias("cost_rounded_percent_rank_cume_dist"),
        F.dense_rank().over(w9).alias("brand_avg_cost_whole_percent_rank_dense_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "cost_rounded",
        "cost_per_stock_qty",
    )
    .orderBy(F.col("brand").asc_nulls_last())
)
