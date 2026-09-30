from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").asc_nulls_first(), "id")
w_running = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy("city").orderBy(F.col("rating").desc())
w_grp = Window.orderBy(F.col("rating").desc_nulls_last())
w5 = Window.orderBy(F.col("rating").asc_nulls_last())
w6 = Window.partitionBy("city", "country")
w7 = Window.partitionBy("country")

latest = (
    df_suppliers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

kept_keys = (
    latest
    .select("country", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("rating") < 3.1)
        .select("country", "city")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "dedup_rank",
        F.dense_rank().over(w_running).alias("rating_dense_rank"),
        F.cume_dist().over(w3).alias("rating_cume_dist"),
        F.percent_rank().over(w_grp).alias("rating_percent_rank"),
        F.dense_rank().over(w5).alias("rating_dense_rank_2"),
    )
)

scored_suppliers = (
    scored
    .filter(F.col("rating_dense_rank").between(5, 8))
    .select(
        "rating",
        "dedup_rank",
        "rating_dense_rank_2",
        F.max("rating").over(w6).alias("city_max_rating"),
        F.count("rating_percent_rank").over(w7).alias("country_count_rating_percent_rank"),
    )
)

result = (
    scored_suppliers
    .groupBy(
        F.when(F.col("city_max_rating") >= 4, "PREMIUM").otherwise("STANDARD").alias("city_max_rating_tier"),
    )
    .agg(
        F.percentile_approx("rating_dense_rank_2", 0.95).alias("rating_dense_rank_2_p95"),
        F.percentile_approx("rating_dense_rank_2", [0.25, 0.5, 0.75]).alias("rating_dense_rank_2_quartiles"),
    )
    .filter(F.col("rating_dense_rank_2_p95") > 0)
    .orderBy(F.col("city_max_rating_tier").desc())
)
