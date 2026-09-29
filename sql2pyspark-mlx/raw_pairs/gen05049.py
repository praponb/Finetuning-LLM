from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("created_at").desc(), "id", "attr_key")
w_running = Window.partitionBy("attr_key").orderBy("is_utm_source", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("attr_key").orderBy("is_utm_source", "id", "attr_key")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

latest = (
    exploded
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest_carts = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "attr_key", "v")
)

cleaned = (
    latest_carts
    .filter(F.col("attr_key") == "utm_source")
    .withColumn("attr_key_title", F.initcap(F.trim("attr_key")))
    .withColumn("clean_attr_key", F.trim("attr_key"))
)

cleaned_carts = (
    cleaned
    .filter(F.col("attr_key").eqNullSafe(F.lit("gift")))
    .select("*", F.upper("attr_key").alias("attr_key_upper"))
)

filtered = (
    cleaned_carts
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "attr_key",
        "attr_key_title",
        "attr_key_upper",
        F.upper("attr_key").alias("attr_key_upper_2"),
        F.when(F.col("attr_key") == "utm_source", 1).otherwise(0).alias("is_utm_source"),
        F.coalesce("attr_key", F.lit("NONE")).alias("resolved_attr_key"),
    )
)

with_windows = (
    filtered
    .filter(F.col("is_utm_source") > 67)
    .select(
        "id",
        "attr_key",
        "attr_key_title",
        "attr_key_upper",
        "attr_key_upper_2",
        "resolved_attr_key",
        F.min("is_utm_source").over(w_running).alias("running_min_is_utm_source"),
        F.sum("is_utm_source").over(w_running).alias("running_sum_is_utm_source"),
        F.max("is_utm_source").over(w_running).alias("running_max_is_utm_source"),
        F.avg("is_utm_source").over(w_running).alias("running_avg_is_utm_source"),
        F.lead("is_utm_source", 1).over(w_grp).alias("next_is_utm_source"),
    )
)

stats = (
    with_windows
    .groupBy(
        F.when(F.col("running_min_is_utm_source") >= 83, "PREMIUM").otherwise("STANDARD").alias("running_min_is_utm_source_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("attr_key") == "device").alias("device_events"),
        F.round(F.sum("running_avg_is_utm_source") / F.nullif(F.count(F.when(F.col("attr_key") == "weight_kg", 1)), F.lit(0)), 2).alias("running_avg_is_utm_source_per_weight_kg"),
        F.max("running_avg_is_utm_source").alias("max_running_avg_is_utm_source"),
    )
    .filter(F.col("device_events") >= 1)
)

result = (
    stats
    .select(
        "running_min_is_utm_source_tier",
        "device_events",
        "running_avg_is_utm_source_per_weight_kg",
        "max_running_avg_is_utm_source",
    )
    .orderBy(F.col("running_min_is_utm_source_tier").desc())
)
