from pyspark.sql import functions as F

prepared_tariffs = (
    df_tariff_windows
    .filter(F.col("tariff") == "TOU")
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.date_format(F.col("valid_to"), "yyyy-MM-dd").alias("valid_to_label"),
    )
)

stats = (
    prepared_tariffs
    .groupBy(F.date_trunc("month", "valid_from").alias("valid_from_month"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("rate"), 2).alias("total_rate"),
        F.sum(F.when(F.col("tariff") == "EV", 1).otherwise(0)).alias("n_tariff_ev"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
        F.sum("rate").alias("sum_rate_check"),
    )
    .filter(F.col("sum_rate_check") > 2.2)
    .drop("sum_rate_check")
)

exploded = (
    stats
    .select(
        "valid_from_month",
        "distinct_ids",
        "total_rate",
        "n_tariff_ev",
        F.explode("tariff_list").alias("tariff_list_item"),
    )
)

scored = (
    exploded
    .select(
        "valid_from_month",
        "distinct_ids",
        "total_rate",
        "n_tariff_ev",
        "tariff_list_item",
        F.to_date(F.col("valid_from_month")).alias("valid_from_month_date"),
        F.abs(F.col("n_tariff_ev") - 4).alias("n_tariff_ev_distance"),
    )
)

result = (
    scored
    .filter(F.col("n_tariff_ev") >= 4)
    .select(
        "valid_from_month",
        "n_tariff_ev",
        "tariff_list_item",
        "valid_from_month_date",
        "n_tariff_ev_distance",
        F.least(F.col("n_tariff_ev_distance"), F.col("total_rate")).alias("least_n_tariff_ev_distance_total_rate"),
    )
    .orderBy(
        F.col("valid_from_month_date").asc(),
        "valid_from_month",
        "tariff_list_item",
    )
    .limit(5)
)
