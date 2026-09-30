from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").asc_nulls_first())
w2 = Window.partitionBy("city").orderBy(F.col("age").asc_nulls_first())
w3 = Window.partitionBy("city")
w4 = Window.orderBy(F.col("age").desc_nulls_last())
w5 = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_first())
w6 = Window.partitionBy("city").orderBy(F.col("age_share").desc_nulls_last())
w7 = Window.partitionBy("city").orderBy(F.col("age_cume_dist").desc_nulls_last())
w8 = Window.orderBy(F.col("city_stddev_samp_age").desc_nulls_last())

base = (
    df_customers
    .filter(F.col("city").isNotNull() & F.col("email").like("%@startup.io"))
    .select(
        "*",
        F.initcap(F.trim("city")).alias("city_title"),
        F.initcap(F.trim("segment")).alias("segment_title"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.amount") <= 550), "left_anti")
)

deduped = (
    kept
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "name", "email", "city", "is_active", "age", "segment_title")
)

kept_keys = (
    deduped
    .select("id", "city")
    .subtract(
        df_customers
        .filter(F.col("email").like("%@company.co"))
        .select("id", "city")
    )
)

verified = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "name",
        "city",
        "age",
        "segment_title",
        F.cume_dist().over(w2).alias("age_cume_dist"),
        F.round((F.col("age") / F.sum("age").over(w3)), 4).alias("age_share"),
        F.rank().over(w4).alias("age_rank"),
        F.rank().over(w5).alias("age_rank_2"),
        F.stddev_samp("age").over(w3).alias("city_stddev_samp_age"),
    )
)

enriched = (
    scored
    .select(
        "segment_title",
        "age_cume_dist",
        "age_rank_2",
        "city_stddev_samp_age",
        F.dense_rank().over(w6).alias("age_share_dense_rank"),
        F.cume_dist().over(w7).alias("age_cume_dist_cume_dist"),
        F.dense_rank().over(w8).alias("city_stddev_samp_age_dense_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("age_cume_dist") < 0.25)
    .withColumn("city_stddev_samp_age_dense_rank_times_age_cume_dist", (F.col("city_stddev_samp_age_dense_rank") * F.col("age_cume_dist")))
    .withColumn("city_stddev_samp_age_dense_rank_plus_age_cume_dist_cume_dist", (F.col("city_stddev_samp_age_dense_rank") + F.col("age_cume_dist_cume_dist")))
)

summary = (
    valid
    .groupBy(
        F.when(F.col("city_stddev_samp_age_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("city_stddev_samp_age_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("city_stddev_samp_age_dense_rank_plus_age_cume_dist_cume_dist", 0.95).alias("city_stddev_samp_age_dense_rank_plus_age_cume_dist_cume_dist_p95"),
        F.percentile_approx("age_share_dense_rank", [0.25, 0.5, 0.75]).alias("age_share_dense_rank_quartiles"),
        F.percentile_approx("age_cume_dist_cume_dist", 0.9).alias("age_cume_dist_cume_dist_p90"),
        F.percentile_approx("age_cume_dist_cume_dist", 0.9).alias("age_cume_dist_cume_dist_p90_2"),
    )
)

result = (
    summary
    .select(
        "city_stddev_samp_age_dense_rank_tier",
        "age_share_dense_rank_quartiles",
        "age_cume_dist_cume_dist_p90",
        "age_cume_dist_cume_dist_p90_2",
    )
    .orderBy(F.col("age_cume_dist_cume_dist_p90").desc_nulls_first())
)
