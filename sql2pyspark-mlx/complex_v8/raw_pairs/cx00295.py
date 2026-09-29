from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_prior_2 = Window.orderBy("start_date", "id").rowsBetween(-2, -1)
w_status = Window.partitionBy("status")
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_end_date_year = Window.partitionBy(F.year("end_date"))
w_all_by_start_date_prior_3 = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(-3, -1)
w_product_holder_id_by_start_d = Window.partitionBy("product", "holder_id").orderBy("start_date", "id")
w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_status_by_end_date_p = Window.partitionBy("product", "status").orderBy("end_date", "id").rowsBetween(-3, -1)
w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc())

scoped = (
    df_policies
    .filter((F.col("start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-07-07").cast("date"))) & (F.col("holder_id") <= 16))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.round(F.avg("premium").over(w_all_by_start_date_prior_2), 2).alias("premium_avg_2_prior"),
        F.max("premium").over(w_status).alias("status_max_premium"),
        F.row_number().over(w_holder_id_by_end_date).alias("rn"),
        F.size(F.collect_set("status").over(w_end_date_year)).alias("distinct_status_in_end_date_year"),
        F.round(F.avg("premium").over(w_all_by_start_date_prior_3), 2).alias("premium_avg_3_prior"),
        F.lead("premium", 1, 0).over(w_product_holder_id_by_start_d).alias("next_premium"),
        F.lag("end_date", 2).over(w_all_by_start_date).alias("prev2_end_date"),
        F.min("premium").over(w_all_by_start_date_running).alias("min_premium_so_far"),
        F.sum("premium").over(w_product_status_by_end_date_p).alias("premium_sum_3_prior"),
        F.count(F.lit(1)).over(w_all_by_start_date_running).alias("running_count"),
        F.round(F.percent_rank().over(w_all_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.rank().over(w_all_by_start_date_ties).alias("start_date_rank"),
    )
)

result = (
    sequenced
    .filter(F.col("premium_avg_2_prior") < 3560.0)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        "premium_avg_2_prior",
        "rn",
        "distinct_status_in_end_date_year",
        "premium_avg_3_prior",
        "next_premium",
        "min_premium_so_far",
        "premium_sum_3_prior",
        "running_count",
        "start_date_percent_rank",
        "start_date_rank",
    )
    .orderBy(F.col("premium_avg_3_prior").desc_nulls_first(), "status")
)
