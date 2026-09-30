from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "opened_date"), "region").orderBy("opened_date", "id").rowsBetween(-6, -1)
w_running = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w_hist = Window.orderBy(F.col("sqft_avg_6_prior").desc_nulls_last())
w4 = Window.orderBy(F.col("sqft_bucket").desc())

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("region") != "West"))
    .select(
        "*",
        F.when(F.col("sqft") >= 12330, "High").when(F.col("sqft") >= 4278, "Medium").otherwise("Low").alias("sqft_tier"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.opened_date")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.opened_date")), "left_semi")
)

metrics = (
    eligible
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "sqft_tier",
        F.avg("sqft").over(w).alias("sqft_avg_6_prior"),
        F.ntile(10).over(w_running).alias("sqft_bucket"),
    )
)

matched_keys = (
    metrics
    .select("region")
    .intersectAll(
        df_stores
        .filter(F.col("city").isin(["New York", "London"]))
        .select("region")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "sqft_avg_6_prior",
        "sqft_bucket",
        F.cume_dist().over(w_hist).alias("sqft_avg_6_prior_cume_dist"),
        F.dense_rank().over(w4).alias("sqft_bucket_dense_rank"),
    )
)

result = with_windows.select("id", "sqft", "sqft_avg_6_prior", "sqft_bucket")
