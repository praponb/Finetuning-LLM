from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows_by_last_read_at_ties = Window.partitionBy("n_rows").orderBy(F.col("last_read_at").desc())
w_quality_n_rows = Window.partitionBy("quality", "n_rows")
w_n_rows_by_last_read_at_full = Window.partitionBy("n_rows").orderBy(F.col("last_read_at").desc(), "quality").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(8, 14))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

totals = (
    base
    .filter(F.col("site_id") >= 5)
    .groupBy("quality")
    .agg(
        F.round(F.sum("kwh"), 2).alias("total_kwh"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("quality") == "BAD", F.col("kwh"))).alias("kwh_quality_bad"),
        F.round(F.avg("kwh"), 2).alias("avg_kwh"),
        F.max("read_at").alias("last_read_at"),
        F.sum("kwh").alias("total_kwh_2"),
    )
    .filter(F.col("n_rows") >= 2)
)

ranked = (
    totals
    .select(
        "*",
        F.round(F.percent_rank().over(w_n_rows_by_last_read_at_ties), 3).alias("last_read_at_percent_rank"),
        F.round(F.col("total_kwh") / F.nullif(F.sum("total_kwh").over(w_quality_n_rows), F.lit(0)), 4).alias("total_kwh_share_of_quality_n_rows"),
        F.last("total_kwh").over(w_n_rows_by_last_read_at_full).alias("last_total_kwh"),
        F.sum("n_rows").over(w_quality_n_rows).alias("quality_n_rows_sum_n_rows"),
    )
)

annotated = (
    ranked
    .filter(F.col("last_read_at_percent_rank") >= 0.1)
    .select(
        "*",
        F.concat(F.col("quality"), F.lit("#"), F.col("n_rows").cast("string")).alias("quality_ref"),
        F.when(F.col("total_kwh_2") >= 576.0, "A").when(F.col("total_kwh_2") >= 252.0, "B").otherwise("C").alias("total_kwh_2_tier"),
        F.abs(F.col("kwh_quality_bad") - 360.0).alias("kwh_quality_bad_distance"),
    )
)

result = (
    annotated
    .cube("total_kwh_2_tier", "n_rows", "quality")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("total_kwh_2_tier").alias("distinct_total_kwh_2_tiers"),
        F.count("*").alias("cnt"),
        F.round(F.sum("last_read_at_percent_rank"), 2).alias("total_last_read_at_percent_rank"),
    )
    .orderBy(
        F.col("total_kwh_2_tier").asc_nulls_last(),
        F.col("n_rows").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
