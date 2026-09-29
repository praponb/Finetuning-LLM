from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("team").orderBy(F.col("level").desc(), F.col("id").desc())
w_all_by_level = Window.orderBy(F.col("level").desc(), "id")
w_level_tercile_level_rank = Window.partitionBy("level_tercile", "level_rank")
w_level_by_level_tercile = Window.partitionBy("level").orderBy(F.col("level_tercile").desc(), "id")
w_max_level_n_rows_by_n_rows_p = Window.partitionBy("max_level", "n_rows").orderBy("n_rows_per_level_rank", "level_rank")

filtered_adjusters = (
    df_adjusters
    .filter(F.length(F.trim(F.col("adjuster_name"))) >= 4)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

deduped = (
    filtered_adjusters
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_windows = (
    deduped
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.ntile(3).over(w_all_by_level).alias("level_tercile"),
        F.rank().over(w_all_by_level).alias("level_rank"),
    )
)

sequenced = (
    with_windows
    .select(
        "*",
        F.round(F.col("level_rank") / F.nullif(F.sum("level_rank").over(w_level_tercile_level_rank), F.lit(0)), 4).alias("level_rank_share_of_level_tercile_level_rank"),
        (F.col("level") - F.lag("level").over(w_level_by_level_tercile)).alias("level_change"),
        F.count(F.lit(1)).over(w_level_tercile_level_rank).alias("rows_in_level_tercile_level_rank"),
        F.size(F.collect_set("team").over(w_level_tercile_level_rank)).alias("distinct_team_in_level_tercile_level_rank"),
    )
)

stats = (
    sequenced
    .filter(F.col("level_change") <= 0)
    .groupBy("level_rank")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("level").alias("max_level"),
        F.sum("rows_in_level_tercile_level_rank").alias("sum_rows_in_level_tercile_level_rank_check"),
    )
    .filter(F.col("sum_rows_in_level_tercile_level_rank_check") > 5)
    .drop("sum_rows_in_level_tercile_level_rank_check")
)

annotated = (
    stats
    .select(
        "level_rank",
        "n_rows",
        "max_level",
        F.round(F.col("n_rows") / F.nullif(F.col("level_rank"), F.lit(0)), 3).alias("n_rows_per_level_rank"),
        F.when(F.col("level_rank") >= 8, "A").when(F.col("level_rank") >= 3, "B").otherwise("C").alias("level_rank_tier"),
    )
)

windowed = (
    annotated
    .select(
        "level_rank",
        "max_level",
        "n_rows_per_level_rank",
        "level_rank_tier",
        (F.col("n_rows_per_level_rank") - F.lag("n_rows_per_level_rank").over(w_max_level_n_rows_by_n_rows_p)).alias("n_rows_per_level_rank_change"),
        F.ntile(3).over(w_max_level_n_rows_by_n_rows_p).alias("n_rows_per_level_rank_tercile"),
    )
)

result = (
    windowed
    .select(
        "level_rank",
        "level_rank_tier",
        "n_rows_per_level_rank_change",
        "n_rows_per_level_rank_tercile",
        (F.col("n_rows_per_level_rank") + F.col("max_level")).alias("n_rows_per_level_rank_plus_max_level"),
        F.round(F.col("n_rows_per_level_rank") * 1.5, 2).alias("n_rows_per_level_rank_adj"),
    )
    .orderBy(
        F.col("n_rows_per_level_rank_adj").asc_nulls_last(),
        F.col("level_rank").desc(),
    )
    .limit(20)
)
