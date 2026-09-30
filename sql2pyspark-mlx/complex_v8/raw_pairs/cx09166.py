from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy("read_at", "id")
w_all_by_quality_len = Window.orderBy("quality_len", "id")

scoped = (
    df_meter_readings
    .filter(F.col("site_id") <= 6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

sequenced = (
    scoped
    .select(
        "id",
        "read_at",
        "quality_len",
        "kwh_bucket",
        F.lead("read_at", 2).over(w_all_by_read_at).alias("next2_read_at"),
        F.lead("quality_len").over(w_all_by_quality_len).alias("next_quality_len"),
        (F.col("kwh_bucket") - F.lag("kwh_bucket").over(w_all_by_quality_len)).alias("kwh_bucket_change"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_quality_len)).alias("kwh_change"),
    )
)

tagged = (
    sequenced
    .select(
        "id",
        "read_at",
        "quality_len",
        "next_quality_len",
        "kwh_bucket_change",
        (F.floor(F.col("kwh_bucket") / 25) * 25).alias("kwh_bucket_bucket"),
        F.date_trunc("hour", F.col("next2_read_at")).alias("next2_read_at_hour"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

filtered = (
    tagged
    .crossJoin(tagged.agg(F.avg("quality_len").alias("overall_avg_quality_len")))
    .filter(F.col("quality_len") > F.col("overall_avg_quality_len"))
    .drop("overall_avg_quality_len")
)

result = (
    filtered
    .rollup("next_quality_len", "read_at_dayofweek")
    .agg(
        F.round(F.avg("next_quality_len"), 2).alias("avg_next_quality_len"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quality_len", 0.5).alias("median_quality_len"),
    )
    .orderBy(
        F.col("next_quality_len").asc_nulls_last(),
        F.col("read_at_dayofweek").asc_nulls_last(),
    )
)
