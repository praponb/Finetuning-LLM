from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff = Window.partitionBy("tariff")
w_tariff_by_rate_ties = Window.partitionBy("tariff").orderBy("rate")

filtered = (
    df_tariff_windows
    .filter(F.month(F.col("valid_to")).isin(2, 3))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.round(F.avg("rate").over(w_tariff), 2).alias("tariff_avg_rate"),
        F.round(F.cume_dist().over(w_tariff_by_rate_ties), 3).alias("rate_cume_dist"),
        F.sum("rate").over(w_tariff).alias("tariff_sum_rate"),
    )
)

per_group = (
    metrics
    .groupBy(
        F.when(F.col("rate") >= 5.7, "HIGH").when(F.col("rate") >= 4.2, "MEDIUM").otherwise("LOW").alias("rate_tier"),
        F.date_trunc("month", "valid_from").alias("valid_from_month"),
        "tariff",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.min("valid_from").alias("first_valid_from"),
        F.sum(F.when(F.col("tariff").isin("TOU", "FLAT"), 1).otherwise(0)).alias("n_tariff_in_tou_flat"),
        F.countDistinct("tariff").alias("distinct_tariffs"),
        F.max("rate").alias("max_rate"),
        F.avg("tariff_avg_rate").alias("avg_tariff_avg_rate_check"),
    )
    .filter(F.col("avg_tariff_avg_rate_check") > 4.1)
    .drop("avg_tariff_avg_rate_check")
)

result = (
    per_group
    .groupBy("distinct_tariffs", "n_tariff_in_tou_flat")
    .pivot("tariff", ["EV", "TOU", "FLAT"])
    .agg(F.avg("n_rows"))
    .orderBy("distinct_tariffs", "n_tariff_in_tou_flat")
)
