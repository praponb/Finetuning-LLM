from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value", "k").orderBy(F.col("is_welcome5").desc_nulls_first())
w2 = Window.partitionBy("k")
w_hist = Window.partitionBy("k").orderBy("is_welcome5_cume_dist")
w4 = Window.partitionBy("k", "value")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

exploded_carts = (
    exploded
    .select("id", "k", "attr_value", F.explode_outer("coupon_codes").alias("value"))
)

base = (
    exploded_carts
    .filter(F.col("value").isNotNull() & F.col("k").isin(["weight_kg", "utm_source"]))
    .select(
        "*",
        F.when(F.col("value") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.cume_dist().over(w).alias("is_welcome5_cume_dist"),
        F.sum("is_welcome5").over(w2).alias("k_sum_is_welcome5"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "k",
        "value",
        "is_welcome5",
        "k_sum_is_welcome5",
        F.dense_rank().over(w_hist).alias("is_welcome5_cume_dist_dense_rank"),
        F.max("is_welcome5_cume_dist").over(w4).alias("k_max_is_welcome5_cume_dist"),
    )
)

result = (
    enriched
    .rollup(
        F.when(F.col("k_max_is_welcome5_cume_dist") >= 1, "HIGH").otherwise("LOW").alias("k_max_is_welcome5_cume_dist_tier"),
        "value",
        F.when(F.col("k_sum_is_welcome5") >= 373, "LARGE").otherwise("SMALL").alias("k_sum_is_welcome5_tier"),
    )
    .agg(
        F.round(F.sum("is_welcome5") / F.nullif(F.count(F.when(F.col("value") == "WELCOME5", 1)), F.lit(0)), 2).alias("is_welcome5_per_welcome5"),
        F.round(F.avg("k_max_is_welcome5_cume_dist"), 2).alias("avg_k_max_is_welcome5_cume_dist"),
        F.countDistinct("k").alias("distinct_ks"),
        F.count_if(F.col("value") == "WELCOME5").alias("welcome5_events"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("k") == "weight_kg").alias("weight_kg_events"),
    )
    .filter(F.col("is_welcome5_per_welcome5") > 0)
    .orderBy(F.col("avg_k_max_is_welcome5_cume_dist").desc_nulls_first())
)
