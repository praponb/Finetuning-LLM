from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_group_site_id_by_rea = Window.partitionBy("quality_group", "site_id").orderBy("read_at")
w_site_id = Window.partitionBy("site_id")
w_quality_group_site_id_by_rea_2 = Window.partitionBy("quality_group", "site_id").orderBy("read_at", "id")
w_site_id_quality_group_by_vol = Window.partitionBy("site_id", "quality_group").orderBy("voltage", "id")
w_quality_group_quality_len_by = Window.partitionBy("quality_group", "quality_len").orderBy("read_at_percent_rank")
w_quality_group_quality_len_by_2 = Window.partitionBy("quality_group", "quality_len").orderBy("read_at_percent_rank", "id")

cleaned_readings = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

tagged = (
    cleaned_readings
    .filter(F.col("kwh_filled") < 102.0)
    .select(
        "*",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

metrics = (
    tagged
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_len",
        "quality_group",
        F.round(F.cume_dist().over(w_quality_group_site_id_by_rea), 3).alias("read_at_cume_dist"),
        F.round(F.col("kwh_filled") / F.nullif(F.sum("kwh_filled").over(w_site_id), F.lit(0)), 4).alias("kwh_filled_share_of_site_id"),
        F.round(F.percent_rank().over(w_quality_group_site_id_by_rea_2), 3).alias("read_at_percent_rank"),
        F.ntile(10).over(w_site_id_quality_group_by_vol).alias("voltage_decile"),
    )
)

with_trends = (
    metrics
    .withColumn("read_at_percent_rank_dense_rank", F.dense_rank().over(w_quality_group_quality_len_by))
    .withColumn("read_at_percent_rank_dense_rank_2", F.dense_rank().over(w_quality_group_quality_len_by_2))
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality_len",
        "voltage_decile",
        "read_at_percent_rank_dense_rank",
        "read_at_percent_rank_dense_rank_2",
        F.month(F.col("read_at")).alias("read_at_month"),
        F.coalesce(F.col("read_at_percent_rank"), F.lit(0.0)).alias("read_at_percent_rank_filled"),
    )
    .orderBy(F.col("quality_len").desc(), F.col("kwh").asc())
)
