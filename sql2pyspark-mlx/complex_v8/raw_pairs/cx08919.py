from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_start_date_year_2 = Window.partitionBy("cnt", "start_date_year_2")
w_distinct_status_by_distinct = Window.partitionBy("distinct_status").orderBy("distinct_status", "start_date_year_ym", "start_date_year_2").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_status_by_distinct_2 = Window.partitionBy("distinct_status").orderBy("distinct_status", "start_date_year_ym", "start_date_year_2")
w_start_date_year_2_by_distinc = Window.partitionBy("start_date_year_2").orderBy("distinct_status", "start_date_year_ym", "start_date_year_2").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_year_2_by_distinc_2 = Window.partitionBy("start_date_year_2").orderBy("distinct_status", "start_date_year_ym", "start_date_year_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_policies
    .filter(F.col("product") == "HOME")
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

per_group = (
    cleaned
    .filter(F.hour(F.col("start_date_year")).between(6, 12))
    .groupBy(
        F.date_format("start_date_year", "yyyy-MM").alias("start_date_year_ym"),
        F.year("start_date").alias("start_date_year_2"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("status").alias("distinct_status"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
)

with_trends = (
    per_group
    .select(
        "start_date_year_ym",
        "start_date_year_2",
        "cnt",
        F.round(F.col("start_date_year_2") / F.nullif(F.sum("start_date_year_2").over(w_cnt_start_date_year_2), F.lit(0)), 4).alias("start_date_year_2_share_of_cnt_start_date_year_2"),
        F.nth_value("start_date_year_2", 2).over(w_distinct_status_by_distinct).alias("second_start_date_year_2"),
        F.lag("distinct_status").over(w_distinct_status_by_distinct_2).alias("prev_distinct_status"),
        F.last("cnt").over(w_start_date_year_2_by_distinc).alias("last_cnt"),
        F.lead("distinct_status").over(w_distinct_status_by_distinct_2).alias("next_distinct_status"),
        F.first("distinct_status", ignorenulls=True).over(w_start_date_year_2_by_distinc_2).alias("first_known_distinct_status"),
    )
)

result = (
    with_trends
    .select(
        "start_date_year_ym",
        "start_date_year_2",
        "second_start_date_year_2",
        "first_known_distinct_status",
    )
    .unpivot(
        ids=["start_date_year_ym", "start_date_year_2"],
        values=["second_start_date_year_2", "first_known_distinct_status"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("start_date_year_ym", "start_date_year_2", "metric_name")
)
