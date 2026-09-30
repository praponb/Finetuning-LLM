from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id_by_end_date = Window.partitionBy("status", "holder_id").orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_holder_joined_on_year = Window.partitionBy(F.year("holder_joined_on"))
w_holder_region_product_by_hol = Window.partitionBy("holder_region", "product").orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(-1, -1)
w_all_by_days_end_date_to_star = Window.orderBy(F.col("days_end_date_to_start_date").desc_nulls_first(), "id")
w_status_holder_id_by_end_date_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("end_date").asc_nulls_last())
w_all_by_days_end_date_to_star_2 = Window.orderBy(F.col("days_end_date_to_start_date_percent_rank").desc(), "id")
w_distinct_holder_region_in_ho = Window.partitionBy("distinct_holder_region_in_holder_joined_on_year", "end_date_rank")
w_all_by_days_end_date_to_star_3 = Window.orderBy("days_end_date_to_start_date", "id")

base = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-12-05").cast("date"), F.lit("2027-05-16").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
    )
)

enriched_holder = (
    base.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

windowed = (
    enriched_holder
    .select(
        "*",
        F.round(F.avg("premium").over(w_status_holder_id_by_end_date), 2).alias("premium_avg_6_prior"),
        F.round(F.col("days_end_date_to_start_date") / F.nullif(F.sum("days_end_date_to_start_date").over(w_holder_joined_on_year), F.lit(0)), 4).alias("days_end_date_to_start_date_share_of_holder_join"),
        F.sum("days_end_date_to_start_date").over(w_holder_region_product_by_hol).alias("days_end_date_to_start_date_sum_1_prior"),
        F.count(F.lit(1)).over(w_holder_joined_on_year).alias("rows_in_holder_joined_on_year"),
        F.round(F.percent_rank().over(w_all_by_days_end_date_to_star), 3).alias("days_end_date_to_start_date_percent_rank"),
        F.row_number().over(w_all_by_days_end_date_to_star).alias("days_end_date_to_start_date_seq"),
        F.size(F.collect_set("holder_region").over(w_holder_joined_on_year)).alias("distinct_holder_region_in_holder_joined_on_year"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_joined_on_year), F.lit(0)), 4).alias("premium_share_of_holder_joined_on_year"),
        F.round(F.avg("days_end_date_to_start_date").over(w_holder_joined_on_year), 2).alias("holder_joined_on_year_avg_days_end_date_to_start"),
        F.rank().over(w_status_holder_id_by_end_date_2).alias("end_date_rank"),
    )
)

top_rows = (
    windowed
    .filter(F.col("days_end_date_to_start_date_seq") <= 3)
    .drop("days_end_date_to_start_date_seq")
)

windowed_2 = (
    top_rows
    .select(
        "*",
        F.lag("premium").over(w_all_by_days_end_date_to_star_2).alias("prev_premium"),
        F.size(F.collect_set("holder_region").over(w_distinct_holder_region_in_ho)).alias("distinct_holder_region_in_distinct_holder_region"),
        F.round(F.col("days_end_date_to_start_date") / F.nullif(F.sum("days_end_date_to_start_date").over(w_distinct_holder_region_in_ho), F.lit(0)), 4).alias("days_end_date_to_start_date_share_of_distinct_ho"),
        F.ntile(10).over(w_all_by_days_end_date_to_star_3).alias("days_end_date_to_start_date_decile"),
        F.lag("premium_avg_6_prior").over(w_all_by_days_end_date_to_star_2).alias("prev_premium_avg_6_prior"),
        F.count(F.lit(1)).over(w_distinct_holder_region_in_ho).alias("rows_in_distinct_holder_region_in_holder_joined_"),
    )
)

totals = (
    windowed_2
    .groupBy(
        "distinct_holder_region_in_holder_joined_on_year",
        F.date_format("holder_joined_on", "yyyy-MM").alias("holder_joined_on_ym"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("holder_region") == "North", 1)) / F.count("*"), 1).alias("pct_holder_region_north"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("product").isNotNull(), 1)).alias("n_product_is_not_null"),
        F.percentile_approx("holder_joined_on_year_avg_days_end_date_to_start", 0.5).alias("median_holder_joined_on_year_avg_days_end_date_t"),
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
        F.percentile_approx("rows_in_distinct_holder_region_in_holder_joined_", 0.5).alias("median_rows_in_distinct_holder_region_in_holder_"),
        F.max("distinct_holder_region_in_holder_joined_on_year").alias("max_distinct_holder_region_in_holder_joined_on_y"),
    )
    .filter(F.col("max_distinct_holder_region_in_holder_joined_on_y") > 5)
    .drop("max_distinct_holder_region_in_holder_joined_on_y")
)

result = (
    totals
    .select(
        "distinct_holder_region_in_holder_joined_on_year",
        "holder_joined_on_ym",
        "pct_holder_region_north",
        "cnt",
        "median_holder_joined_on_year_avg_days_end_date_t",
        "end_date_span_days",
        "median_rows_in_distinct_holder_region_in_holder_",
        F.round(F.col("pct_holder_region_north") * 0.25, 2).alias("pct_holder_region_north_adj"),
        F.least(F.col("cnt"), F.col("n_product_is_not_null")).alias("least_cnt_n_product_is_not_null"),
    )
    .orderBy(F.col("distinct_holder_region_in_holder_joined_on_year").desc())
)
