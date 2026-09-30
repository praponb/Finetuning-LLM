from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_by_joined_on_month_r = Window.partitionBy("segment").orderBy(F.col("joined_on_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_segment_or_unknown_by_joined = Window.partitionBy("segment_or_unknown").orderBy("joined_on_month")
w_region_segment_or_unknown_by = Window.partitionBy("region", "segment_or_unknown").orderBy(F.col("joined_on_month").desc(), "id")

scoped = (
    df_policyholders
    .filter(F.col("region") == "East")
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
        F.month(F.col("joined_on")).alias("joined_on_month"),
        F.substring(F.trim(F.col("holder_name")), 1, 4).alias("holder_name_prefix"),
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment_or_unknown"),
    )
)

qualified = (
    scoped.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.premium") > 1042.5), "left_anti")
)

metrics = (
    qualified
    .select(
        "*",
        F.first("joined_on_month", ignorenulls=True).over(w_segment_by_joined_on_month_r).alias("first_known_joined_on_month"),
        F.round(F.percent_rank().over(w_segment_or_unknown_by_joined), 3).alias("joined_on_month_percent_rank"),
        F.rank().over(w_region_segment_or_unknown_by).alias("joined_on_month_rank"),
    )
)

grouped = (
    metrics
    .groupBy("joined_on_month", "joined_on_month_rank")
    .agg(
        F.round(F.sum("first_known_joined_on_month") / F.nullif(F.sum("joined_on_month_percent_rank"), F.lit(0)), 3).alias("first_known_joined_on_month_to_joined_on_month_p"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("segment") == "SME", 1).otherwise(0)).alias("n_segment_sme"),
        F.sum("joined_on_month").alias("sum_joined_on_month_check"),
    )
    .filter(F.col("sum_joined_on_month_check") > 4)
    .drop("sum_joined_on_month_check")
)

enriched = (
    grouped
    .filter(F.col("joined_on_month") >= 5)
    .select(
        "joined_on_month",
        "joined_on_month_rank",
        "first_known_joined_on_month_to_joined_on_month_p",
        "n_segment_sme",
        F.abs(F.col("cnt") - 8).alias("cnt_distance"),
    )
)

qualified_2 = (
    enriched
    .crossJoin(enriched.agg(F.max("n_segment_sme").alias("overall_max_n_segment_sme")))
    .filter(F.col("n_segment_sme") < F.col("overall_max_n_segment_sme"))
    .drop("overall_max_n_segment_sme")
)

tagged = (
    qualified_2
    .withColumn("least_cnt_distance_joined_on_month", F.least(F.col("cnt_distance"), F.col("joined_on_month")))
)

result = (
    tagged
    .filter(F.col("first_known_joined_on_month_to_joined_on_month_p") <= 7.5)
    .select(
        "joined_on_month",
        "joined_on_month_rank",
        "n_segment_sme",
        "cnt_distance",
        "least_cnt_distance_joined_on_month",
    )
    .orderBy(
        F.col("joined_on_month_rank").desc(),
        F.col("least_cnt_distance_joined_on_month").asc(),
    )
)
