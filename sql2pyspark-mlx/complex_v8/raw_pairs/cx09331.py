from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_or_unknown_tariff_gro = Window.partitionBy("tariff_or_unknown", "tariff_group").orderBy(F.col("rate").desc())
w_tariff_group = Window.partitionBy("tariff_group")
w_dedup = Window.partitionBy("tariff_or_unknown").orderBy("rate_adj", "id")
w_distinct_tariff_in_tariff_gr = Window.partitionBy("distinct_tariff_in_tariff_group", F.to_date("valid_to")).orderBy(F.col("distinct_tariff_in_tariff_group").desc_nulls_first(), "id")
w_rate_dense_rank = Window.partitionBy("rate_dense_rank")

filtered = (
    df_tariff_windows
    .filter((F.col("rate") < 6.1) & (F.col("valid_to") < F.lit("2025-03-12 23:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.round(F.col("rate") * 0.25, 2).alias("rate_adj"),
        F.when(F.col("tariff").isin("TOU", "FLAT"), "group_a").when(F.col("tariff") == "EV", "group_b").otherwise("other").alias("tariff_group"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_tariff_or_unknown_tariff_gro).alias("rate_dense_rank"),
        F.size(F.collect_set("tariff").over(w_tariff_group)).alias("distinct_tariff_in_tariff_group"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest
    .select(
        "id",
        "valid_to",
        "rate",
        "tariff_or_unknown",
        "rate_adj",
        "tariff_group",
        F.ntile(3).over(w_distinct_tariff_in_tariff_gr).alias("distinct_tariff_in_tariff_group_tercile"),
        F.round(F.col("rate") - F.avg("rate").over(w_rate_dense_rank), 2).alias("rate_vs_rate_dense_rank_avg"),
        F.lead("rate_dense_rank", 1, 0).over(w_distinct_tariff_in_tariff_gr).alias("next_rate_dense_rank"),
        F.row_number().over(w_distinct_tariff_in_tariff_gr).alias("seq_no"),
        F.round(F.percent_rank().over(w_distinct_tariff_in_tariff_gr), 3).alias("distinct_tariff_in_tariff_group_percent_rank"),
    )
)

top_per_group = ranked.filter(F.col("seq_no") == 1)

filtered_2 = (
    top_per_group
    .crossJoin(top_per_group.agg(F.min("rate_vs_rate_dense_rank_avg").alias("overall_min_rate_vs_rate_dense_rank_avg")))
    .filter(F.col("rate_vs_rate_dense_rank_avg") > F.col("overall_min_rate_vs_rate_dense_rank_avg"))
    .drop("overall_min_rate_vs_rate_dense_rank_avg")
)

result = (
    filtered_2
    .select(
        "id",
        "valid_to",
        "rate",
        "tariff_or_unknown",
        "rate_adj",
        "tariff_group",
        "rate_vs_rate_dense_rank_avg",
        "seq_no",
        "distinct_tariff_in_tariff_group_percent_rank",
    )
)
