from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_city_kuala_lumpur = Window.partitionBy("n_city_kuala_lumpur")
w_all_by_distinct_city_in_n_ci = Window.orderBy(F.col("distinct_city_in_n_city_kuala_lumpur").desc_nulls_first(), "city")
w_all_by_distinct_city_in_n_ci_2 = Window.orderBy("distinct_city_in_n_city_kuala_lumpur", "city")
w_city = Window.partitionBy("city")

prepared_warehouses = (
    df_warehouses
    .filter((F.col("city").isin("Singapore", "Kuala Lumpur")) | (F.col("capacity").between(2614, 11506)))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.lower(F.regexp_replace(F.trim(F.col("city")), " ", "_")).alias("city_slug"),
    )
)

rollup_base = (
    prepared_warehouses
    .groupBy("city")
    .agg(
        F.datediff(F.max("opened_on"), F.min("opened_on")).alias("opened_on_span_days"),
        F.sum(F.when(F.col("city") == "Kuala Lumpur", 1).otherwise(0)).alias("n_city_kuala_lumpur"),
        F.sum("capacity").alias("total_capacity"),
    )
)

without_matches = (
    rollup_base
    .crossJoin(rollup_base.agg(F.min("total_capacity").alias("overall_min_total_capacity")))
    .filter(F.col("total_capacity") > F.col("overall_min_total_capacity"))
    .drop("overall_min_total_capacity")
)

ranked = (
    without_matches
    .withColumn("distinct_city_in_n_city_kuala_lumpur", F.size(F.collect_set("city").over(w_n_city_kuala_lumpur)))
    .withColumn("n_city_kuala_lumpur_sum_opened_on_span_days", F.sum("opened_on_span_days").over(w_n_city_kuala_lumpur))
)

with_windows = (
    ranked
    .select(
        "city",
        "n_city_kuala_lumpur",
        "n_city_kuala_lumpur_sum_opened_on_span_days",
        F.lead("n_city_kuala_lumpur").over(w_all_by_distinct_city_in_n_ci).alias("next_n_city_kuala_lumpur"),
        F.lead("n_city_kuala_lumpur", 3, 0).over(w_all_by_distinct_city_in_n_ci_2).alias("next3_n_city_kuala_lumpur"),
        F.row_number().over(w_all_by_distinct_city_in_n_ci).alias("distinct_city_in_n_city_kuala_lumpur_seq"),
        F.round(F.avg("total_capacity").over(w_city), 2).alias("city_avg_total_capacity"),
        F.round(F.col("n_city_kuala_lumpur") / F.nullif(F.sum("n_city_kuala_lumpur").over(w_city), F.lit(0)), 4).alias("n_city_kuala_lumpur_share_of_city"),
        F.min("opened_on_span_days").over(w_city).alias("city_min_opened_on_span_days"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["distinct_city_in_n_city_kuala_lumpur_seq", "n_city_kuala_lumpur"], ["distinct_city_in_n_city_kuala_lumpur_seq"], []],
        "distinct_city_in_n_city_kuala_lumpur_seq", "n_city_kuala_lumpur",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("city")).alias("city_list"),
    )
    .orderBy(
        F.col("distinct_city_in_n_city_kuala_lumpur_seq").asc_nulls_last(),
        F.col("n_city_kuala_lumpur").asc_nulls_last(),
    )
)
