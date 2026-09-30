from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity_whole").desc_nulls_first())
w_hist = Window.partitionBy("store_id").orderBy("quantity")

cleaned = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 5, 8]))
    .select("id", "store_id", "quantity", F.floor("quantity").alias("quantity_whole"))
)

kept_keys = (
    cleaned
    .select("store_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([3, 5]))
        .select("store_id", "id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "*",
        F.avg("quantity_whole").over(w_part).alias("store_id_avg_quantity_whole"),
        F.stddev_samp("quantity_whole").over(w_part).alias("store_id_stddev_samp_quantity_whole"),
        F.percent_rank().over(w2).alias("quantity_whole_percent_rank"),
        F.sum("quantity").over(w_part).alias("store_id_sum_quantity"),
        F.percent_rank().over(w_hist).alias("quantity_percent_rank"),
    )
)

cleaned_inventory = (
    scored
    .filter(F.col("quantity").between(145, 199))
    .select(
        "*",
        (F.col("quantity_percent_rank") + F.col("quantity")).alias("quantity_percent_rank_plus_quantity"),
    )
)

enriched = (
    cleaned_inventory
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.store_name"))
)

enriched_inventory = (
    enriched
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.city"))
)

result = (
    enriched_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "quantity_whole",
        "store_id_avg_quantity_whole",
        "store_id_stddev_samp_quantity_whole",
        "quantity_whole_percent_rank",
        "store_id_sum_quantity",
        "quantity_percent_rank",
    )
    .orderBy("quantity_whole_percent_rank")
)
