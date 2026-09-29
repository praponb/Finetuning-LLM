from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last())
w3 = Window.orderBy(F.col("rating").desc_nulls_last())
w4 = Window.orderBy(F.col("rating_rank_per_last3_sum_rating").asc_nulls_last())

with_windows = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select(
        "*",
        F.avg("rating").over(w_running).alias("last3_avg_rating"),
        F.count("rating").over(w_running).alias("last3_count_rating"),
        F.sum("rating").over(w_running).alias("last3_sum_rating"),
        F.min("rating").over(w_running).alias("last3_min_rating"),
        F.rank().over(w2).alias("rating_rank"),
    )
)

matched_keys = (
    with_windows
    .select("id", "country")
    .subtract(
        df_suppliers
        .filter(F.col("rating") < 3.3)
        .select("id", "country")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("supplier_name").like("S%"))
    .select(
        "id",
        "supplier_name",
        "rating",
        "last3_sum_rating",
        F.when(F.col("rating") >= 3.7, "High").when(F.col("rating") >= 2.2, "Medium").otherwise("Low").alias("rating_tier"),
        F.round((F.col("rating_rank") / F.col("last3_sum_rating")), 2).alias("rating_rank_per_last3_sum_rating"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.year(F.col("x.launch_date")) >= 2023), "left_semi")
)

metrics = (
    kept
    .withColumn("rating_rank", F.rank().over(w3))
    .withColumn("rating_rank_per_last3_sum_rating_dense_rank", F.dense_rank().over(w4))
)

eligible = (
    metrics
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") == 289), "left_semi")
)

result = (
    eligible
    .rollup(
        F.when(F.col("rating_rank_per_last3_sum_rating_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("rating_rank_per_last3_sum_rating_dense_rank_tier"),
        F.when(F.col("rating_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("rating_rank_tier"),
    )
    .agg(
        F.round(F.avg("rating_rank_per_last3_sum_rating_dense_rank"), 2).alias("avg_rating_rank_per_last3_sum_rating_dense_rank"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.percentile_approx("rating_rank_per_last3_sum_rating", [0.25, 0.5, 0.75]).alias("rating_rank_per_last3_sum_rating_quartiles"),
    )
    .orderBy(F.col("rating_rank_tier").desc())
)
