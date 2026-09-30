from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc_nulls_first())
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("customer_id").orderBy("age")
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("age_share_2").desc_nulls_last())

flattened = (
    df_carts
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

with_dims = (
    flattened
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city").isin(["Tokyo", "London"]))
    .select("a.id", "a.customer_id", F.col("c.country"), F.col("c.age"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "customer_id",
        F.dense_rank().over(w).alias("age_dense_rank"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.dense_rank().over(w3).alias("age_dense_rank_2"),
        F.round((F.col("age") / F.sum("age").over(w4)), 4).alias("age_share_2"),
        F.percent_rank().over(w3).alias("age_percent_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "customer_id",
        "age_share",
        "age_dense_rank_2",
        "age_share_2",
        "age_percent_rank",
        F.cume_dist().over(w5).alias("age_share_2_cume_dist"),
    )
)

cleaned = (
    enriched
    .filter(F.col("age_dense_rank_2") >= 5)
    .select(
        "id",
        "customer_id",
        "age_share",
        "age_dense_rank_2",
        "age_percent_rank",
        "age_share_2_cume_dist",
        F.round((F.col("age_percent_rank") / F.col("age_dense_rank_2")), 2).alias("age_percent_rank_per_age_dense_rank_2"),
    )
)

result = (
    cleaned
    .withColumn("age_percent_rank_per_age_dense_rank_2_tier", F.when(F.col("age_percent_rank_per_age_dense_rank_2") >= 82, "LARGE").otherwise("SMALL"))
    .withColumn("age_share_tier", F.when(F.col("age_share") >= 0.89, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["age_percent_rank_per_age_dense_rank_2_tier", "age_share_tier"], ["age_percent_rank_per_age_dense_rank_2_tier"]],
        "age_percent_rank_per_age_dense_rank_2_tier",
        "age_share_tier",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("age_share", 0.5).alias("age_share_p50"),
        F.percentile_approx("age_share_2_cume_dist", 0.5).alias("age_share_2_cume_dist_p50"),
        F.count("*").alias("cnt_2"),
        F.grouping_id().alias("grouping_id"),
    )
)
