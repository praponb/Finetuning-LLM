from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_read_at_prior_3 = Window.orderBy("read_at", "id").rowsBetween(-3, -1)
w_all_by_kwh = Window.orderBy(F.col("kwh").desc_nulls_first(), "id")
w_read_at_to_date_by_kwh = Window.partitionBy(F.to_date("read_at")).orderBy("kwh", "id")
w_all_by_read_at = Window.orderBy("read_at", "id")
w_read_at_to_date_by_kwh_prior = Window.partitionBy(F.to_date("read_at")).orderBy("kwh", "id").rowsBetween(-1, -1)

scoped_readings = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("kwh") >= 104.8, "HIGH").when(F.col("kwh") >= 42.1, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

metrics = (
    scoped_readings
    .select(
        "*",
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id_quality), F.lit(0)), 4).alias("kwh_share_of_site_id_quality"),
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.size(F.collect_set("kwh_tier").over(w_site_id_quality)).alias("distinct_kwh_tier_in_site_id_quality"),
        F.round(F.avg("kwh").over(w_all_by_read_at_prior_3), 2).alias("kwh_avg_3_prior"),
        F.lag("read_at", 2).over(w_all_by_kwh).alias("prev2_read_at"),
        F.round(F.avg("kwh").over(w_site_id_quality), 2).alias("site_id_quality_avg_kwh"),
        F.max("kwh").over(w_site_id_quality).alias("site_id_quality_max_kwh"),
        F.rank().over(w_read_at_to_date_by_kwh).alias("kwh_rank"),
        F.row_number().over(w_read_at_to_date_by_kwh).alias("rn"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.ntile(3).over(w_all_by_kwh).alias("kwh_tercile"),
        F.sum("kwh").over(w_read_at_to_date_by_kwh_prior).alias("kwh_sum_1_prior"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.when(F.col("rn") >= 7, "A").when(F.col("rn") >= 3, "B").otherwise("C").alias("rn_tier"),
        F.coalesce(F.col("rows_in_site_id_quality"), F.lit(0)).alias("rows_in_site_id_quality_filled"),
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
)

result = (
    enriched
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "kwh_tier",
        "kwh_share_of_site_id_quality",
        "rows_in_site_id_quality",
        "distinct_kwh_tier_in_site_id_quality",
        "kwh_avg_3_prior",
        "prev2_read_at",
        "site_id_quality_avg_kwh",
        "site_id_quality_max_kwh",
        "kwh_rank",
        "rn",
        "minutes_since_prev_read_at",
        "kwh_tercile",
        "rn_tier",
        "rows_in_site_id_quality_filled",
        "read_at_quarter",
    )
    .orderBy(
        F.col("quality").desc(),
        F.col("kwh_share_of_site_id_quality").asc(),
        "id",
    )
    .limit(20)
)
