from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_last())
w2 = Window.partitionBy("city").orderBy(F.col("sqft_filled").asc_nulls_first())
w3 = Window.orderBy(F.col("sqft").desc())
w4 = Window.partitionBy("region", "city")

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-03-09"))) & F.col("region").isNotNull())
    .select(
        "*",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

kept_keys = (
    filtered
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("region") != "Central")
        .select("id", "city")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.cume_dist().over(w).alias("sqft_cume_dist"),
        F.rank().over(w2).alias("sqft_filled_rank"),
        F.dense_rank().over(w3).alias("sqft_dense_rank"),
    )
)

metrics_stores = (
    metrics
    .filter(F.col("city").isin(["Bangkok", "Berlin"]))
    .select(
        "id",
        "city",
        "region",
        "sqft_filled_rank",
        F.count("sqft_dense_rank").over(w4).alias("region_count_sqft_dense_rank"),
    )
)

stats = (
    metrics_stores
    .groupBy(
        F.when(F.col("sqft_filled_rank") >= 8, "HIGH").otherwise("LOW").alias("sqft_filled_rank_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sort_array(F.collect_set("region")).alias("region_list_2"),
        F.percentile_approx("sqft_filled_rank", [0.25, 0.5, 0.75]).alias("sqft_filled_rank_quartiles"),
    )
)

result = stats.select("sqft_filled_rank_tier", "region_list", "region_list_2")
