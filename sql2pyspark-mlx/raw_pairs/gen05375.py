from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last())
w2 = Window.orderBy(F.col("rating_dense_rank").desc_nulls_last())
w_part = Window.orderBy(F.col("rating_dense_rank").asc_nulls_last())

kept_keys = (
    df_suppliers
    .select("country")
    .intersect(
        df_suppliers
        .filter(F.length("supplier_name") > 9)
        .select("country")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.month(F.col("x.launch_date")).isin([3, 6, 8])), "left_anti")
)

enriched = (
    eligible
    .select("id", "supplier_name", F.dense_rank().over(w).alias("rating_dense_rank"))
)

with_windows = (
    enriched
    .filter(F.col("rating_dense_rank").between(3, 7))
    .select(
        "*",
        F.cume_dist().over(w2).alias("rating_dense_rank_cume_dist"),
        F.percent_rank().over(w_part).alias("rating_dense_rank_percent_rank"),
    )
)

result = (
    with_windows
    .rollup(
        F.when(F.col("rating_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("rating_dense_rank_tier"),
        F.when(F.col("rating_dense_rank_cume_dist") >= 0.77, "HIGH").otherwise("LOW").alias("rating_dense_rank_cume_dist_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("rating_dense_rank_cume_dist"), 2).alias("avg_rating_dense_rank_cume_dist"),
        F.percentile_approx("rating_dense_rank_percent_rank", 0.95).alias("rating_dense_rank_percent_rank_p95"),
    )
)
