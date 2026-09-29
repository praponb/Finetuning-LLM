from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product", "holder_id").orderBy("start_date", "id")
w_holder_id_by_end_date_dayofw = Window.partitionBy("holder_id").orderBy(F.col("end_date_dayofweek").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_end_date_dayofw_2 = Window.partitionBy("holder_id").orderBy(F.col("end_date_dayofweek").desc(), "id")

prepared = (
    df_policies
    .filter(F.col("product") != "HOME")
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
    )
)

latest_per_key = (
    prepared
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined_holder = (
    latest_per_key.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.status_clean",
        "s.end_date_dayofweek",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

with_trends = (
    joined_holder
    .select(
        "*",
        F.count(F.lit(1)).over(w_holder_id_by_end_date_dayofw).alias("running_count"),
        F.round(F.percent_rank().over(w_holder_id_by_end_date_dayofw_2), 3).alias("end_date_dayofweek_percent_rank"),
    )
)

with_details_holder = (
    with_trends.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score_2"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

scored = (
    with_details_holder
    .filter(F.col("end_date") >= F.lit("2025-12-05").cast("date"))
    .select(
        "*",
        F.coalesce(F.col("holder_region"), F.lit("UNKNOWN")).alias("holder_region_or_unknown"),
        F.round(F.col("holder_risk_score") * 0.9, 2).alias("holder_risk_score_adj"),
    )
)

tagged = scored.withColumn("status_clean_clean", F.initcap(F.trim(F.col("status_clean"))))

result = (
    tagged
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "status_clean",
        "end_date_dayofweek",
        "holder_region",
        "end_date_dayofweek_percent_rank",
        "holder_segment",
        "holder_risk_score_2",
        "holder_region_or_unknown",
        "holder_risk_score_adj",
        "status_clean_clean",
        F.abs(F.col("running_count") - 10).alias("running_count_distance"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
    .orderBy("status", "status_clean_clean")
)
