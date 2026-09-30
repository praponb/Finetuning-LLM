from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_month_tariff = Window.partitionBy(F.month("valid_from"), "tariff")
w_valid_from_month_by_valid_to = Window.partitionBy(F.date_trunc("month", "valid_from")).orderBy(F.col("valid_to").desc(), "id").rowsBetween(-3, -1)
w_valid_from_month_by_valid_to_2 = Window.partitionBy(F.date_trunc("month", "valid_from")).orderBy(F.col("valid_to").desc(), "id")

cleaned = (
    df_tariff_windows
    .filter(F.col("tariff") == "TOU")
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.round((F.unix_timestamp(F.col("valid_to")) - F.unix_timestamp(F.col("valid_from"))) / 3600, 1).alias("hours_valid_from_to_valid_to"),
        F.round((F.unix_timestamp(F.col("valid_from")) - F.unix_timestamp(F.col("valid_to"))) / 3600, 1).alias("hours_valid_to_to_valid_from"),
        F.month(F.col("valid_to")).alias("valid_to_month"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.max("hours_valid_to_to_valid_from").over(w_valid_from_month_tariff).alias("valid_from_month_tariff_max_hours_valid_to_to_va"),
        F.sum("valid_to_month").over(w_valid_from_month_by_valid_to).alias("valid_to_month_sum_3_prior"),
        (F.col("hours_valid_from_to_valid_to") - F.lag("hours_valid_from_to_valid_to").over(w_valid_from_month_by_valid_to_2)).alias("hours_valid_from_to_valid_to_change"),
    )
)

totals = (
    metrics
    .filter(F.col("rate").between(2.2, 5.9))
    .groupBy("tariff", "valid_to_month", F.year("valid_from").alias("valid_from_year"))
    .agg(
        F.round(F.avg("hours_valid_from_to_valid_to_change"), 2).alias("avg_hours_valid_from_to_valid_to_change"),
        F.count(F.when(F.col("tariff") == "FLAT", 1)).alias("n_tariff_flat"),
        F.min("valid_to").alias("first_valid_to"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
        F.sum("valid_to_month").alias("total_valid_to_month"),
    )
)

result = (
    totals
    .groupBy("valid_from_year")
    .pivot("tariff", ["EV", "TOU", "FLAT"])
    .agg(F.min("total_valid_to_month"))
    .orderBy("valid_from_year")
)
