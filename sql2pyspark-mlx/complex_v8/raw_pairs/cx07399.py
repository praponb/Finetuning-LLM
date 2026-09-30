from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh_filled = Window.partitionBy("site_id").orderBy("kwh_filled", "id")
w_all_by_quality_len_full = Window.orderBy(F.col("quality_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_len = Window.partitionBy("quality_len")
w_site_id_quality_len_by_read = Window.partitionBy("site_id", "quality_len").orderBy(F.col("read_at").desc(), "id")
w_site_id_quality_len_by_read_2 = Window.partitionBy("site_id", "quality_len").orderBy(F.col("read_at").desc())

recent_readings = (
    df_meter_readings
    .filter((F.col("site_id") <= 4) | (F.col("quality").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

sequenced = (
    recent_readings
    .select(
        "*",
        (F.col("kwh_filled") - F.lag("kwh_filled").over(w_site_id_by_kwh_filled)).alias("kwh_filled_change"),
        F.first("kwh").over(w_all_by_quality_len_full).alias("first_kwh"),
        F.size(F.collect_set("site_id").over(w_quality_len)).alias("distinct_site_id_in_quality_len"),
        F.round(F.percent_rank().over(w_site_id_by_kwh_filled), 3).alias("kwh_filled_percent_rank"),
        F.count(F.lit(1)).over(w_quality_len).alias("rows_in_quality_len"),
        F.lead("quality_len").over(w_site_id_quality_len_by_read).alias("next_quality_len"),
        F.dense_rank().over(w_site_id_quality_len_by_read_2).alias("read_at_dense_rank"),
        F.lead("kwh").over(w_site_id_quality_len_by_read).alias("next_kwh"),
    )
)

result = (
    sequenced
    .filter(F.col("kwh_filled") < 66.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "kwh_filled",
        "quality_len",
        "distinct_site_id_in_quality_len",
        "rows_in_quality_len",
        "next_quality_len",
        "next_kwh",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        (F.floor(F.col("next_quality_len") / 2) * 2).alias("next_quality_len_bucket"),
    )
    .orderBy("quality")
)
