from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_group = Window.partitionBy("tariff_group")
w_all_by_rate = Window.orderBy(F.col("rate").desc(), "id")
w_distinct_tariff_in_tariff_gr = Window.partitionBy("distinct_tariff_in_tariff_group", "tariff_group_2").orderBy(F.col("rate_vs_tariff_group_avg").desc(), "id").rowsBetween(-1, -1)
w_all_by_rate_full = Window.orderBy(F.col("rate").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_group_tariff_by_rate = Window.partitionBy("tariff_group", "tariff").orderBy(F.col("rate_vs_tariff_group_avg").desc(), "id").rowsBetween(-1, Window.currentRow)
w_distinct_tariff_in_tariff_gr_2 = Window.partitionBy("distinct_tariff_in_tariff_group", "tariff_group_2").orderBy(F.col("rate_vs_tariff_group_avg").desc(), "id").rowsBetween(-6, -1)
w_tariff_distinct_tariff_group = Window.partitionBy("tariff", "distinct_tariff_group_2_in_tariff_group")
w_distinct_tariff_group_2_in_t = Window.partitionBy("distinct_tariff_group_2_in_tariff_group").orderBy("rate_vs_tariff_group_avg_avg_6_prior")
w_tariff_group_2 = Window.partitionBy("tariff_group_2")
w_distinct_tariff_group_2_in_t_2 = Window.partitionBy("distinct_tariff_group_2_in_tariff_group").orderBy("rate_vs_tariff_group_avg_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_rate_seq_by_greatest = Window.partitionBy("tariff", "rate_seq").orderBy("greatest_rate_avg_2_rate_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_tariff_windows
    .filter((F.col("valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp")) | (~F.col("tariff").isin("FLAT", "EV")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.when(F.col("tariff").isin("EV", "FLAT"), "group_a").when(F.col("tariff") == "TOU", "group_b").otherwise("other").alias("tariff_group"),
        F.when(F.col("tariff").isin("FLAT", "TOU"), "group_a").when(F.col("tariff") == "EV", "group_b").otherwise("other").alias("tariff_group_2"),
    )
)

filtered = (
    recent
    .crossJoin(recent.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

ranked = (
    filtered
    .select(
        "*",
        F.round(F.col("rate") - F.avg("rate").over(w_tariff_group), 2).alias("rate_vs_tariff_group_avg"),
        F.size(F.collect_set("tariff_group_2").over(w_tariff_group)).alias("distinct_tariff_group_2_in_tariff_group"),
        F.size(F.collect_set("tariff").over(w_tariff_group)).alias("distinct_tariff_in_tariff_group"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.row_number().over(w_all_by_rate).alias("rate_seq"),
        F.max("rate_vs_tariff_group_avg").over(w_distinct_tariff_in_tariff_gr).alias("rate_vs_tariff_group_avg_max_1_prior"),
        F.last("valid_from").over(w_all_by_rate_full).alias("last_valid_from"),
        F.round(F.avg("rate").over(w_tariff_group_tariff_by_rate), 2).alias("rate_avg_2"),
        F.round(F.avg("rate_vs_tariff_group_avg").over(w_distinct_tariff_in_tariff_gr_2), 2).alias("rate_vs_tariff_group_avg_avg_6_prior"),
        F.sum("rate_vs_tariff_group_avg").over(w_tariff_distinct_tariff_group).alias("tariff_distinct_tariff_group_2_in_tariff_group_s"),
    )
)

annotated = (
    ranked_2
    .filter(F.col("tariff_group") == "other")
    .select(
        "*",
        F.greatest(F.col("rate_avg_2"), F.col("rate_seq")).alias("greatest_rate_avg_2_rate_seq"),
        (F.col("valid_to") + F.expr("INTERVAL 7 DAYS")).alias("valid_to_plus_7d"),
    )
)

windowed = (
    annotated
    .select(
        "*",
        F.rank().over(w_distinct_tariff_group_2_in_t).alias("rate_vs_tariff_group_avg_avg_6_prior_rank"),
        F.count(F.lit(1)).over(w_tariff_group_2).alias("rows_in_tariff_group_2"),
        F.last("valid_from").over(w_distinct_tariff_group_2_in_t_2).alias("last_valid_from_2"),
        F.last("valid_from", ignorenulls=True).over(w_tariff_rate_seq_by_greatest).alias("last_known_valid_from"),
    )
)

result = (
    windowed
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        "tariff_group",
        "rate_vs_tariff_group_avg",
        "distinct_tariff_group_2_in_tariff_group",
        "distinct_tariff_in_tariff_group",
        "rate_seq",
        "last_valid_from",
        "rate_avg_2",
        "rate_vs_tariff_group_avg_avg_6_prior",
        "tariff_distinct_tariff_group_2_in_tariff_group_s",
        "greatest_rate_avg_2_rate_seq",
        "valid_to_plus_7d",
        "rate_vs_tariff_group_avg_avg_6_prior_rank",
        "rows_in_tariff_group_2",
        "last_valid_from_2",
    )
    .orderBy(
        F.col("tariff_distinct_tariff_group_2_in_tariff_group_s").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
