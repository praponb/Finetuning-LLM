from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_region_end_date_year = Window.partitionBy("holder_region", "end_date_year")
w_all_by_premium_running = Window.orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_end_date_year_2 = Window.partitionBy("holder_region", "end_date_year").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)
w_holder_region_end_date_year_3 = Window.partitionBy("holder_region", "end_date_year").orderBy("end_date")
w_holder_region_end_date_year_4 = Window.partitionBy("holder_region", "end_date_year").orderBy("end_date", "id")
w_all_by_premium_moving_3 = Window.orderBy(F.col("premium").desc(), "id").rowsBetween(-3, Window.currentRow)
w_distinct_holder_id_in_holder = Window.partitionBy("distinct_holder_id_in_holder_region_end_date_yea").orderBy(F.col("first_known_end_date_year").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_status_in_holder_re = Window.partitionBy("distinct_status_in_holder_region_end_date_year").orderBy("end_date_year_sum_7", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_status_in_holder_re_2 = Window.partitionBy("distinct_status_in_holder_region_end_date_year").orderBy("end_date_year_sum_7")
w_end_date_rank_by_holder_regi = Window.partitionBy("end_date_rank").orderBy("holder_region_end_date_year_avg_end_date_year", "id").rowsBetween(-6, Window.currentRow)
w_end_date_month_holder_region = Window.partitionBy(F.month("end_date"), "holder_region")
w_holder_region_distinct_holde = Window.partitionBy("holder_region", "distinct_holder_id_in_holder_region_end_date_yea").orderBy(F.col("end_date_year_sum_7").desc(), "id")
w_distinct_status_in_holder_re_3 = Window.partitionBy("distinct_status_in_holder_region_end_date_year").orderBy("end_date_year_sum_7", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_policies
    .filter(F.col("product").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
        F.year(F.col("end_date")).alias("end_date_year"),
    )
)

with_parent_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.region").alias("holder_region"), "p.holder_name")
)

sequenced = (
    with_parent_holder
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "product_prefix",
        "end_date_year",
        "holder_region",
        "holder_name",
        F.round(F.avg("end_date_year").over(w_holder_region_end_date_year), 2).alias("holder_region_end_date_year_avg_end_date_year"),
        F.first("end_date_year", ignorenulls=True).over(w_all_by_premium_running).alias("first_known_end_date_year"),
        F.size(F.collect_set("status").over(w_holder_region_end_date_year)).alias("distinct_status_in_holder_region_end_date_year"),
        F.sum("end_date_year").over(w_holder_region_end_date_year_2).alias("end_date_year_sum_7"),
        F.rank().over(w_holder_region_end_date_year_3).alias("end_date_rank"),
        F.ntile(4).over(w_holder_region_end_date_year_4).alias("end_date_quartile"),
        F.round(F.col("end_date_year") - F.avg("end_date_year").over(w_holder_region_end_date_year), 2).alias("end_date_year_vs_holder_region_end_date_year_avg"),
        F.size(F.collect_set("holder_id").over(w_holder_region_end_date_year)).alias("distinct_holder_id_in_holder_region_end_date_yea"),
        F.round(F.avg("end_date_year").over(w_all_by_premium_moving_3), 2).alias("end_date_year_avg_4"),
    )
)

first_rows = sequenced.filter(F.col("end_date_rank") <= 5)

windowed = (
    first_rows
    .select(
        "id",
        "holder_id",
        "end_date",
        "holder_region",
        "holder_name",
        "first_known_end_date_year",
        "distinct_status_in_holder_region_end_date_year",
        "end_date_year_sum_7",
        "end_date_quartile",
        "end_date_year_vs_holder_region_end_date_year_avg",
        "end_date_year_avg_4",
        F.sum("end_date_year").over(w_distinct_holder_id_in_holder).alias("running_end_date_year"),
        F.nth_value("end_date_year_avg_4", 2).over(w_distinct_status_in_holder_re).alias("second_end_date_year_avg_4"),
        F.rank().over(w_distinct_status_in_holder_re_2).alias("end_date_year_sum_7_rank"),
        F.max("end_date_quartile").over(w_end_date_rank_by_holder_regi).alias("end_date_quartile_max_7"),
        F.max("end_date_year").over(w_end_date_month_holder_region).alias("end_date_month_holder_region_max_end_date_year"),
        F.lag("end_date_year_sum_7", 3).over(w_holder_region_distinct_holde).alias("prev3_end_date_year_sum_7"),
        F.max("end_date_year").over(w_distinct_status_in_holder_re_3).alias("end_date_year_max_7"),
    )
)

result = (
    windowed
    .filter(F.col("end_date_quartile") <= 9)
    .select(
        "id",
        "holder_id",
        "holder_region",
        "holder_name",
        "distinct_status_in_holder_region_end_date_year",
        "end_date_year_sum_7",
        "end_date_quartile",
        "running_end_date_year",
        "second_end_date_year_avg_4",
        "end_date_year_sum_7_rank",
        "end_date_quartile_max_7",
        "end_date_month_holder_region_max_end_date_year",
        "end_date_year_max_7",
    )
    .orderBy(F.col("holder_id").desc(), "id")
    .limit(20)
)
