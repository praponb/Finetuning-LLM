from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w_part = Window.partitionBy("store_id", "product_id").orderBy("quantity")
w3 = Window.partitionBy("store_id", "product_id")
w4 = Window.orderBy(F.col("quantity").desc_nulls_last())
w5 = Window.partitionBy("product_id").orderBy(F.col("quantity_dense_rank").desc_nulls_last())
w6 = Window.orderBy(F.col("store_id_min_quantity").desc_nulls_first())
w7 = Window.orderBy(F.col("store_id_min_quantity").asc_nulls_first())
w8 = Window.orderBy(F.col("quantity_dense_rank").desc())
w9 = Window.partitionBy("product_id", "store_id")
w10 = Window.partitionBy("product_id")

latest = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    latest
    .select("store_id")
    .exceptAll(
        df_inventory
        .filter(F.col("product_id").isin([4, 6, 10]))
        .select("store_id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w_part).alias("quantity_dense_rank"),
        F.min("quantity").over(w3).alias("store_id_min_quantity"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w4).alias("quantity_percent_rank"),
        F.percent_rank().over(w5).alias("quantity_dense_rank_percent_rank"),
        F.rank().over(w6).alias("store_id_min_quantity_rank"),
        F.cume_dist().over(w7).alias("store_id_min_quantity_cume_dist"),
        F.rank().over(w8).alias("quantity_dense_rank_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("last_restock") > "2025-05-01")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "row_num",
        "quantity_dense_rank",
        "quantity_percent_rank",
        "store_id_min_quantity_rank",
        "store_id_min_quantity_cume_dist",
        "quantity_dense_rank_rank",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_adjusted"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.category"), F.col("d.launch_date"), F.col("d.stock_qty"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.avg("quantity").over(w9).alias("product_id_avg_quantity"),
        F.round((F.col("store_id_min_quantity_rank") / F.sum("store_id_min_quantity_rank").over(w10)), 4).alias("store_id_min_quantity_rank_share"),
    )
)

result = (
    with_windows
    .groupBy("store_id", "product_id")
    .pivot("category", ["Sports", "Books"])
    .agg(
        F.avg("store_id_min_quantity_rank_share").alias("amount"),
        F.count(F.lit(1)).alias("n"),
    )
)
