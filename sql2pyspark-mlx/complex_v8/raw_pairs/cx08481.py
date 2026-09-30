from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year_product_by_pre = Window.partitionBy(F.year("end_date"), "product").orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id = Window.partitionBy("holder_id")
w_end_date_month_product_by_st = Window.partitionBy(F.date_trunc("month", "end_date"), "product").orderBy(F.col("start_date").desc())
w_end_date_month_product_by_st_2 = Window.partitionBy(F.date_trunc("month", "end_date"), "product").orderBy(F.col("start_date").desc(), "id")
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_start_date_movi = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(-1, Window.currentRow)
w_end_date_year_product_by_pre_2 = Window.partitionBy(F.year("end_date"), "product").orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_start_date_year_holder_id = Window.partitionBy(F.year("start_date"), "holder_id")
w_all_by_start_date_prior_1 = Window.orderBy("start_date", "id").rowsBetween(-1, -1)
w_end_date_month_product_by_st_3 = Window.partitionBy(F.date_trunc("month", "end_date"), "product").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_stddev_premium_share = Window.orderBy("stddev_premium_share_of_start_date_year_holder_i", "start_date_ym", "next2_end_date_year")

valid_policys = (
    df_policies
    .filter(F.col("premium") <= 4678.8)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

with_trends = (
    valid_policys
    .select(
        "*",
        F.sum("premium").over(w_end_date_year_product_by_pre).alias("running_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
        F.round(F.cume_dist().over(w_end_date_month_product_by_st), 3).alias("start_date_cume_dist"),
        F.lead("end_date", 2).over(w_end_date_month_product_by_st_2).alias("next2_end_date"),
        F.sum("premium").over(w_all_by_start_date_running).alias("running_premium_2"),
        F.max("premium").over(w_holder_id).alias("holder_id_max_premium"),
        F.round(F.avg("premium").over(w_holder_id_by_start_date_movi), 2).alias("premium_avg_2"),
        F.sum("premium").over(w_end_date_year_product_by_pre_2).alias("premium_sum_3_prior"),
        F.count(F.lit(1)).over(w_start_date_year_holder_id).alias("rows_in_start_date_year_holder_id"),
        F.round(F.avg("premium").over(w_all_by_start_date_prior_1), 2).alias("premium_avg_1_prior"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_start_date_year_holder_id), F.lit(0)), 4).alias("premium_share_of_start_date_year_holder_id"),
        F.first("premium", ignorenulls=True).over(w_end_date_month_product_by_st_3).alias("first_known_premium"),
    )
)

summary = (
    with_trends
    .filter(F.col("premium_share_of_holder_id") >= 0.1)
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
        F.year("next2_end_date").alias("next2_end_date_year"),
    )
    .agg(
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
        F.round(F.stddev("premium_share_of_start_date_year_holder_id"), 2).alias("stddev_premium_share_of_start_date_year_holder_i"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.avg("premium_avg_2").alias("avg_premium_avg_2_check"),
    )
    .filter(F.col("avg_premium_avg_2_check") > 1160.0)
    .drop("avg_premium_avg_2_check")
)

sequenced = (
    summary
    .withColumn("stddev_premium_share_of_start_date_year_holder_i_2", F.round(F.cume_dist().over(w_all_by_stddev_premium_share), 3))
    .withColumn("next2_end_date_year_change", F.col("next2_end_date_year") - F.lag("next2_end_date_year").over(w_all_by_stddev_premium_share))
)

result = (
    sequenced
    .select(
        "start_date_ym",
        "next2_end_date_year",
        "end_date_span_days",
        "stddev_premium_share_of_start_date_year_holder_i",
        "product_list",
        "next2_end_date_year_change",
        F.substring(F.trim(F.col("start_date_ym")), 1, 4).alias("start_date_ym_prefix"),
    )
    .orderBy(F.col("next2_end_date_year").desc())
)
