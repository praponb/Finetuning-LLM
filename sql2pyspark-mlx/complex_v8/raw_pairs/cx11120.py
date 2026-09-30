from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_kwh_tier = Window.partitionBy(F.month("read_at"), "kwh_tier")
w_all_by_read_at_moving_3 = Window.orderBy("read_at", "id").rowsBetween(-3, Window.currentRow)
w_all_by_read_at = Window.orderBy("read_at", "id")
w_all_by_read_at_ties = Window.orderBy("read_at")

scoped = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 237.5, "large").when(F.col("voltage") >= 223.3, "medium").otherwise("small").alias("voltage_tier"),
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

tagged = (
    scoped
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "voltage_tier",
        F.when(F.col("kwh") >= 81.1, "HIGH").when(F.col("kwh") >= 28.4, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
    )
)

with_activity = (
    tagged
    .crossJoin(tagged.agg(F.avg("kwh").alias("overall_avg_kwh")))
    .filter(F.col("kwh") > F.col("overall_avg_kwh"))
    .drop("overall_avg_kwh")
)

metrics = (
    with_activity
    .select(
        "id",
        "kwh",
        "quality",
        "voltage_tier",
        F.sum("kwh").over(w_read_at_month_kwh_tier).alias("read_at_month_kwh_tier_sum_kwh"),
        F.max("kwh").over(w_all_by_read_at_moving_3).alias("kwh_max_4"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_read_at)).alias("kwh_change"),
        F.lead("read_at", 3).over(w_all_by_read_at).alias("next3_read_at"),
        F.rank().over(w_all_by_read_at_ties).alias("read_at_rank"),
        F.lag("read_at", 3).over(w_all_by_read_at).alias("prev3_read_at"),
    )
)

summary = (
    metrics
    .groupBy("voltage_tier")
    .agg(
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("n_quality_bad"),
        F.count("*").alias("cnt"),
        F.countDistinct("voltage_tier").alias("distinct_voltage_tiers"),
    )
)

derived = (
    summary
    .select(
        "*",
        F.when(F.col("cnt") >= 8, "HIGH").when(F.col("cnt") >= 4, "MEDIUM").otherwise("LOW").alias("cnt_tier"),
    )
)

screened = (
    derived
    .crossJoin(derived.agg(F.max("n_quality_bad").alias("overall_max_n_quality_bad")))
    .filter(F.col("n_quality_bad") < F.col("overall_max_n_quality_bad"))
    .drop("overall_max_n_quality_bad")
)

result = (
    screened
    .select(
        "voltage_tier",
        "distinct_voltage_tiers",
        "cnt_tier",
        F.concat_ws("|", F.col("cnt_tier"), F.col("voltage_tier")).alias("cnt_tier_voltage_tier_label"),
    )
    .orderBy(F.col("voltage_tier").desc())
)
