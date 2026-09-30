from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_last())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("region").orderBy(F.col("sqft_rank").asc_nulls_first())
w4 = Window.partitionBy("region")
w5 = Window.orderBy(F.col("sqft").desc())

matched_keys = (
    df_stores
    .select("id", "region")
    .intersectAll(
        df_stores
        .filter(F.col("store_name").like("Ma%"))
        .select("id", "region")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.opened_date")), "left_semi")
)

enriched = (
    eligible
    .select(
        "*",
        F.rank().over(w_running).alias("sqft_rank"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.product_id").isin([1, 8, 9])), "left_anti")
)

result = (
    kept
    .select(
        "*",
        F.rank().over(w3).alias("sqft_rank_rank"),
        F.stddev_samp("sqft_rank").over(w4).alias("region_stddev_samp_sqft_rank"),
        F.cume_dist().over(w5).alias("sqft_cume_dist"),
    )
)
