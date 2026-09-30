from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff = Window.partitionBy("tariff")
w_tariff_by_valid_to = Window.partitionBy("tariff").orderBy("valid_to", "id")
w_tariff_by_valid_from_running = Window.partitionBy("tariff").orderBy("valid_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_by_valid_to_ties = Window.partitionBy("tariff").orderBy("valid_to")
w_tariff_by_valid_from = Window.partitionBy("tariff").orderBy("valid_from", "id")
w_tariff_by_valid_to_running = Window.partitionBy("tariff").orderBy("valid_to", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tariffs = (
    df_tariff_windows
    .filter(F.col("rate") <= 5.9)
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

windowed = (
    valid_tariffs
    .select(
        "*",
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_tariff), F.lit(0)), 4).alias("rate_share_of_tariff"),
        F.rank().over(w_tariff_by_valid_to).alias("valid_to_rank"),
        F.max("rate").over(w_tariff_by_valid_from_running).alias("max_rate_so_far"),
        F.max("rate").over(w_tariff).alias("tariff_max_rate"),
        F.rank().over(w_tariff_by_valid_to_ties).alias("valid_to_rank_2"),
        F.count(F.lit(1)).over(w_tariff).alias("rows_in_tariff"),
        F.lead("valid_from", 2).over(w_tariff_by_valid_to).alias("next2_valid_from"),
        F.row_number().over(w_tariff_by_valid_from).alias("seq_no"),
        F.sum("rate").over(w_tariff_by_valid_to_running).alias("running_rate"),
        F.row_number().over(w_tariff_by_valid_to).alias("seq_no_2"),
    )
)

tagged = (
    windowed
    .select(
        "*",
        F.initcap(F.trim(F.col("tariff"))).alias("tariff_clean"),
        (F.col("valid_to") + F.expr("INTERVAL 1 DAYS")).alias("valid_to_plus_1d"),
        F.hour(F.col("next2_valid_from")).alias("next2_valid_from_hour"),
    )
)

stats = (
    tagged
    .groupBy("next2_valid_from_hour")
    .agg(
        F.countDistinct("tariff").alias("distinct_tariffs"),
        F.round(100 * F.count(F.when(F.col("tariff") == "FLAT", 1)) / F.count("*"), 1).alias("pct_tariff_flat"),
        F.count("*").alias("n_rows"),
        F.max("valid_to").alias("last_valid_to"),
        F.round(F.sum("running_rate"), 2).alias("total_running_rate"),
    )
)

result = (
    stats
    .select(
        "next2_valid_from_hour",
        "distinct_tariffs",
        F.to_date(F.col("last_valid_to")).alias("last_valid_to_date"),
        F.least(F.col("next2_valid_from_hour"), F.col("distinct_tariffs")).alias("least_next2_valid_from_hour_distinct_tariffs"),
    )
    .orderBy(
        F.col("last_valid_to_date").desc(),
        F.col("least_next2_valid_from_hour_distinct_tariffs").desc(),
    )
)
