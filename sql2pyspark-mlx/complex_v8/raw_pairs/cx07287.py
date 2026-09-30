from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("quality").orderBy(F.col("voltage_bucket").desc(), F.col("id").desc())

valid = (
    df_meter_readings
    .filter(F.col("voltage") >= 213.7)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
    )
)

latest = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

latest_2 = (
    latest
    .withColumn("rn", F.row_number().over(w_dedup_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

summary = (
    latest_2
    .groupBy("quality_group")
    .agg(
        F.sum(F.when(F.col("quality") == "OK", F.col("voltage")).otherwise(0)).alias("voltage_quality_ok"),
        F.sum(F.when(F.col("quality") != "BAD", F.col("voltage")).otherwise(0)).alias("voltage_quality_bad"),
        F.sum(F.when(F.col("quality") == "OK", F.col("voltage_bucket")).otherwise(0)).alias("voltage_bucket_quality_ok"),
        F.max("voltage_bucket").alias("max_voltage_bucket"),
    )
)

tagged = (
    summary
    .filter(F.col("max_voltage_bucket") < 242)
    .select(
        "*",
        F.length(F.trim(F.col("quality_group"))).alias("quality_group_len"),
        F.upper(F.trim(F.col("quality_group"))).alias("quality_group_clean"),
        F.coalesce(F.col("voltage_quality_bad"), F.lit(0.0)).alias("voltage_quality_bad_filled"),
    )
)

qualified = (
    tagged
    .crossJoin(tagged.agg(F.min("voltage_bucket_quality_ok").alias("overall_min_voltage_bucket_quality_ok")))
    .filter(F.col("voltage_bucket_quality_ok") > F.col("overall_min_voltage_bucket_quality_ok"))
    .drop("overall_min_voltage_bucket_quality_ok")
)

derived = (
    qualified
    .filter(F.col("max_voltage_bucket") > 222)
    .select(
        "*",
        (F.floor(F.col("quality_group_len") / 2) * 2).alias("quality_group_len_bucket"),
        (F.col("voltage_quality_bad") - F.col("voltage_quality_ok")).alias("voltage_quality_bad_minus_voltage_quality_ok"),
        (F.floor(F.col("max_voltage_bucket") / 5) * 5).alias("max_voltage_bucket_bucket"),
    )
)

result = (
    derived
    .select(
        "quality_group",
        "voltage_quality_bad",
        "voltage_bucket_quality_ok",
        "max_voltage_bucket",
        "quality_group_len",
        "quality_group_len_bucket",
        "voltage_quality_bad_minus_voltage_quality_ok",
        "max_voltage_bucket_bucket",
        (F.floor(F.col("max_voltage_bucket") / 5) * 5).alias("max_voltage_bucket_bucket_2"),
        F.least(F.col("max_voltage_bucket"), F.col("quality_group_len_bucket")).alias("least_max_voltage_bucket_quality_group_len_bucke"),
    )
    .orderBy(F.col("voltage_quality_bad").asc(), "quality_group")
    .limit(20)
)
