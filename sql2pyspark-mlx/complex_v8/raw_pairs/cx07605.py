from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())
w_all_by_voltage_full = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality = Window.partitionBy("quality")
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.when(F.col("voltage") >= 232.4, "A").when(F.col("voltage") >= 219.6, "B").otherwise("C").alias("voltage_tier"),
    )
)

tagged = (
    prepared
    .filter(F.col("site_id").isNotNull())
    .withColumn("kwh_tier", F.when(F.col("kwh") >= 110.5, "large").when(F.col("kwh") >= 30.2, "medium").otherwise("small"))
)

latest = (
    tagged
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

metrics = (
    latest
    .select(
        "*",
        F.last("voltage").over(w_all_by_voltage_full).alias("last_voltage"),
        F.sum("kwh").over(w_quality).alias("quality_sum_kwh"),
        F.min("voltage").over(w_all_by_voltage_running).alias("min_voltage_so_far"),
        F.first("kwh").over(w_site_id_by_voltage_full).alias("first_kwh"),
    )
)

result = (
    metrics
    .groupingSets(
        [["site_id", "quality_len", "voltage_tier"], ["site_id"], ["quality_len"]],
        "site_id", "quality_len", "voltage_tier",
    )
    .agg(
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.count("*").alias("row_count"),
        F.percentile_approx("last_voltage", 0.5).alias("median_last_voltage"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("quality_len").asc_nulls_last(),
        F.col("voltage_tier").asc_nulls_last(),
    )
)
