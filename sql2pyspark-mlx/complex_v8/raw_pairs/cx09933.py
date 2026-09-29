from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_quarter_opened_on = Window.partitionBy("opened_on_quarter", F.month("opened_on"))
w_all_by_opened_on = Window.orderBy("opened_on", "id")
w_opened_on_quarter_city_by_ca = Window.partitionBy("opened_on_quarter", "city").orderBy(F.col("capacity").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_all_by_opened_on_ties = Window.orderBy("opened_on")
w_opened_on_quarter_city_by_op = Window.partitionBy("opened_on_quarter", "city").orderBy("opened_on", "id")
w_opened_on_quarter_city_by_ca_2 = Window.partitionBy("opened_on_quarter", "city").orderBy(F.col("capacity").asc_nulls_last())

prepared_warehouses = (
    df_warehouses
    .filter(F.col("opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-07-03").cast("date")))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.quarter(F.col("opened_on")).alias("opened_on_quarter"),
    )
)

with_windows = (
    prepared_warehouses
    .select(
        "*",
        F.size(F.collect_set("city").over(w_opened_on_quarter_opened_on)).alias("distinct_city_in_opened_on_quarter_opened_on_mon"),
        F.round(F.cume_dist().over(w_all_by_opened_on), 3).alias("opened_on_cume_dist"),
        F.round(F.avg("capacity").over(w_opened_on_quarter_city_by_ca), 2).alias("capacity_avg_6_prior"),
        F.dense_rank().over(w_all_by_opened_on_ties).alias("opened_on_dense_rank"),
        F.lead("capacity", 3).over(w_opened_on_quarter_city_by_op).alias("next3_capacity"),
        F.rank().over(w_opened_on_quarter_city_by_ca_2).alias("capacity_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "city",
        "opened_on",
        "distinct_city_in_opened_on_quarter_opened_on_mon",
        "opened_on_cume_dist",
        "capacity_avg_6_prior",
        "opened_on_dense_rank",
        "next3_capacity",
        "capacity_rank",
        F.date_trunc("month", F.col("opened_on")).alias("opened_on_month"),
        F.when(F.col("city").isin("Bangkok", "Kuala Lumpur"), "group_a").when(F.col("city") == "Singapore", "group_b").otherwise("other").alias("city_group"),
    )
    .orderBy("opened_on_cume_dist", F.col("id").desc_nulls_first())
)
