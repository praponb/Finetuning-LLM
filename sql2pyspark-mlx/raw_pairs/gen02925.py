from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("is_version").asc_nulls_first())
w2 = Window.partitionBy("k").orderBy(F.col("is_version").desc())
w3 = Window.partitionBy("k")

exploded = df_app_logs.select("id", F.explode_outer("context").alias("k", "attr_value"))

cleaned = (
    exploded
    .filter(F.col("k").isin(["trace", "version"]))
    .select(
        "*",
        F.when(F.col("k") == "version", 1).otherwise(0).alias("is_version"),
        F.when(F.col("k").isin(["region", "version"]), "Tier 1").otherwise("Other").alias("k_group"),
    )
)

scored = (
    cleaned
    .filter(F.col("is_version") == 50)
    .select(
        "*",
        F.cume_dist().over(w).alias("is_version_cume_dist"),
        F.cume_dist().over(w2).alias("is_version_cume_dist_2"),
        F.cume_dist().over(w).alias("is_version_cume_dist_3"),
        F.sum("is_version").over(w3).alias("k_sum_is_version"),
    )
)

base = (
    scored
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "k",
        "attr_value",
        "k_group",
        "is_version_cume_dist_2",
        "k_sum_is_version",
        F.substring("k", 1, 2).alias("k_prefix"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

result = (
    base
    .withColumn("is_version_cume_dist_2_tier", F.when(F.col("is_version_cume_dist_2") >= 0.64, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["is_version_cume_dist_2_tier", "k"], ["is_version_cume_dist_2_tier"], []],
        "is_version_cume_dist_2_tier",
        "k",
    )
    .agg(
        F.percentile_approx("k_sum_is_version", [0.25, 0.5, 0.75]).alias("k_sum_is_version_quartiles"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_rows"),
        F.grouping("is_version_cume_dist_2_tier").alias("grouping_id"),
    )
)
