from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_slug_len = Window.partitionBy("quality_slug_len")
w_quality_quality_slug_len_by = Window.partitionBy("quality", "quality_slug_len").orderBy("least_read_at_year_total_kwh_2_adj", "quality", "read_at_year")
w_distinct_quality_in_quality = Window.partitionBy("distinct_quality_in_quality_slug_len", "read_at_year").orderBy("distinct_quality_in_quality_slug_len", "quality", "read_at_year").rowsBetween(-3, Window.currentRow)
w_distinct_quality_in_quality_2 = Window.partitionBy("distinct_quality_in_quality_slug_len", "read_at_year")
w_distinct_quality_in_quality_3 = Window.partitionBy("distinct_quality_in_quality_slug_len", "read_at_year").orderBy("distinct_quality_in_quality_slug_len", "quality", "read_at_year")
w_quality_by_quality_slug_len = Window.partitionBy("quality").orderBy(F.col("quality_slug_len").asc_nulls_last())

base_readings = (
    df_meter_readings
    .filter(F.col("site_id") <= 3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

stats = (
    base_readings
    .filter(F.col("read_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
    .groupBy("quality", F.year("read_at").alias("read_at_year"))
    .agg(
        F.round(F.sum("kwh"), 2).alias("total_kwh"),
        F.sum("kwh").alias("total_kwh_2"),
        F.max("kwh").alias("max_kwh_check"),
    )
    .filter(F.col("max_kwh_check") > 16.5)
    .drop("max_kwh_check")
)

annotated = (
    stats
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.round(F.col("total_kwh_2") * 0.9, 2).alias("total_kwh_2_adj"),
    )
)

tagged = (
    annotated
    .filter(F.col("total_kwh_2").between(216.0, 468.0))
    .select(
        "quality",
        "read_at_year",
        "total_kwh",
        "quality_slug",
        F.length(F.trim(F.col("quality_slug"))).alias("quality_slug_len"),
        F.least(F.col("read_at_year"), F.col("total_kwh_2_adj")).alias("least_read_at_year_total_kwh_2_adj"),
        (F.floor(F.col("total_kwh_2") / 100) * 100).alias("total_kwh_2_bucket"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.round(F.avg("read_at_year").over(w_quality_slug_len), 2).alias("quality_slug_len_avg_read_at_year"),
        F.row_number().over(w_quality_quality_slug_len_by).alias("seq_no"),
        F.size(F.collect_set("quality").over(w_quality_slug_len)).alias("distinct_quality_in_quality_slug_len"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.max("total_kwh").over(w_distinct_quality_in_quality).alias("total_kwh_max_4"),
        F.size(F.collect_set("quality").over(w_distinct_quality_in_quality_2)).alias("distinct_quality_in_distinct_quality_in_quality_"),
        F.round(F.percent_rank().over(w_distinct_quality_in_quality_3), 3).alias("distinct_quality_in_quality_slug_len_percent_ran"),
        F.min("quality_slug_len").over(w_distinct_quality_in_quality_2).alias("distinct_quality_in_quality_slug_len_read_at_yea"),
        F.round(F.percent_rank().over(w_quality_by_quality_slug_len), 3).alias("quality_slug_len_percent_rank"),
    )
)

result = (
    sequenced_2
    .filter(F.col("total_kwh") >= 216.0)
    .select(
        "quality",
        "read_at_year",
        "total_kwh",
        "quality_slug",
        "quality_slug_len",
        "least_read_at_year_total_kwh_2_adj",
        "total_kwh_2_bucket",
        "quality_slug_len_avg_read_at_year",
        "seq_no",
        "distinct_quality_in_quality_slug_len",
        "distinct_quality_in_distinct_quality_in_quality_",
        "distinct_quality_in_quality_slug_len_percent_ran",
        "distinct_quality_in_quality_slug_len_read_at_yea",
        "quality_slug_len_percent_rank",
        F.greatest(F.col("read_at_year"), F.col("total_kwh_2_bucket")).alias("greatest_read_at_year_total_kwh_2_bucket"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)
