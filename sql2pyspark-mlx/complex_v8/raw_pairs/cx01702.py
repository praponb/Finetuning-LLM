from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy("read_at", "id", "reading_id")
w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))
w_site_id_rn = Window.partitionBy("site_id", "rn")
w_quality_by_prev2_reading_rea = Window.partitionBy("quality").orderBy("prev2_reading_read_at")
w_quality_by_prev2_reading_rea_2 = Window.partitionBy("quality").orderBy("prev2_reading_read_at", "id", "reading_id")
w_prev2_reading_read_at_dense = Window.partitionBy("prev2_reading_read_at_dense_rank").orderBy("minutes_since_prev_read_at", "id", "reading_id")

cleaned = df_meter_readings.filter(F.col("site_id").isNotNull())

exploded_detail_readings = (
    cleaned.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

with_trends = (
    exploded_detail_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "reading_id",
        "reading_read_at",
        "reading_kwh",
        F.lag("reading_read_at", 2).over(w_all_by_read_at).alias("prev2_reading_read_at"),
        F.lead("kwh", 3, 0).over(w_all_by_read_at).alias("next3_kwh"),
        F.round(F.avg("kwh").over(w_site_id_read_at_to_date), 2).alias("site_id_read_at_to_date_avg_kwh"),
        F.row_number().over(w_all_by_read_at).alias("rn"),
    )
)

scored = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "reading_id",
        "reading_read_at",
        "prev2_reading_read_at",
        "next3_kwh",
        "rn",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.round(F.avg("rn").over(w_site_id_rn), 2).alias("site_id_rn_avg_rn"),
        F.dense_rank().over(w_quality_by_prev2_reading_rea).alias("prev2_reading_read_at_dense_rank"),
        F.round((F.unix_timestamp("reading_read_at") - F.unix_timestamp(F.lag("reading_read_at").over(w_quality_by_prev2_reading_rea_2))) / 60, 1).alias("minutes_since_prev_reading_read_at"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_quality_by_prev2_reading_rea_2))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.percent_rank().over(w_quality_by_prev2_reading_rea), 3).alias("prev2_reading_read_at_percent_rank"),
        F.round(F.col("rn") / F.nullif(F.sum("rn").over(w_site_id_rn), F.lit(0)), 4).alias("rn_share_of_site_id_rn"),
    )
)

sequenced = (
    metrics
    .withColumn("minutes_since_prev_read_at_2", F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_prev2_reading_read_at_dense))) / 60, 1))
    .withColumn("minutes_since_prev_reading_read_at_2", F.round((F.unix_timestamp("reading_read_at") - F.unix_timestamp(F.lag("reading_read_at").over(w_prev2_reading_read_at_dense))) / 60, 1))
)

result = (
    sequenced
    .filter(F.col("quality_slug").isNotNull())
    .select(
        "id",
        "site_id",
        "quality",
        "reading_id",
        "reading_read_at",
        "prev2_reading_read_at",
        "next3_kwh",
        "rn",
        "quality_slug",
        "site_id_rn_avg_rn",
        "prev2_reading_read_at_dense_rank",
        "minutes_since_prev_reading_read_at",
        "minutes_since_prev_read_at",
        "prev2_reading_read_at_percent_rank",
        "minutes_since_prev_read_at_2",
    )
    .orderBy("next3_kwh", "id", "reading_id")
    .limit(5)
)
