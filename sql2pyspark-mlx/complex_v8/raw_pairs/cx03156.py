from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_valid_from_ties = Window.partitionBy("tariff").orderBy("valid_from")
w_tariff = Window.partitionBy("tariff")

prepared = (
    df_tariff_windows
    .filter(F.month(F.col("valid_to")).isin(2, 3))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
    )
)

with_activity = (
    prepared
    .crossJoin(prepared.agg(F.max("rate").alias("overall_max_rate")))
    .filter(F.col("rate") < F.col("overall_max_rate"))
    .drop("overall_max_rate")
)

with_activity_2 = (
    with_activity
    .crossJoin(with_activity.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

ranked = (
    with_activity_2
    .select(
        "id",
        "valid_from",
        F.rank().over(w_tariff_by_valid_from_ties).alias("valid_from_rank"),
        F.round(F.avg("rate").over(w_tariff), 2).alias("tariff_avg_rate"),
        F.dense_rank().over(w_tariff_by_valid_from_ties).alias("valid_from_dense_rank"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_tariff), F.lit(0)), 4).alias("rate_share_of_tariff"),
    )
)

leaders = ranked.filter(F.col("valid_from_dense_rank") <= 3)

leaders_2 = leaders.filter((F.col("valid_from_rank") <= 3) & (F.col("tariff_avg_rate") >= 3.8))

enriched = (
    leaders_2
    .select(
        "*",
        (F.col("valid_from") + F.expr("INTERVAL 1 DAYS")).alias("valid_from_plus_1d"),
        F.date_format(F.col("valid_from"), "yyyy-MM").alias("valid_from_label"),
    )
)

result = (
    enriched
    .select(
        "id",
        "valid_from",
        "valid_from_rank",
        "tariff_avg_rate",
        "valid_from_dense_rank",
        "rate_share_of_tariff",
        "valid_from_plus_1d",
        "valid_from_label",
    )
    .orderBy(
        F.col("valid_from_rank").desc(),
        F.col("valid_from_dense_rank").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
