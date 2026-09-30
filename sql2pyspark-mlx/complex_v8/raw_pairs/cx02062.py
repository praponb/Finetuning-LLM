from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_premium_running = Window.partitionBy("status").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), "id")
w_status_by_premium = Window.partitionBy("status").orderBy("premium", "id")
w_status = Window.partitionBy("status")
w_holder_id_by_holder_joined_o = Window.partitionBy("holder_id").orderBy(F.col("holder_joined_on").asc_nulls_last())
w_holder_segment_by_start_date = Window.partitionBy("holder_segment").orderBy("start_date", "id")
w_holder_segment = Window.partitionBy("holder_segment")
w_status_by_start_date = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")
w_status_by_start_date_running = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next_start_date_month_month = Window.partitionBy(F.date_trunc("month", "next_start_date_month")).orderBy("n_holder_segment_retail", "holder_segment", "next_start_date_month")
w_next_start_date_month_month_2 = Window.partitionBy(F.date_trunc("month", "next_start_date_month")).orderBy("n_holder_segment_retail", "holder_segment", "next_start_date_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_policys = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-10-12").cast("date"), F.lit("2026-09-23").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.date_format(F.col("start_date"), "yyyy-MM-dd").alias("start_date_label"),
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
    )
)

combined_details_holder = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on") < F.lit("2025-04-20").cast("date"))
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

metrics = (
    combined_details_holder
    .select(
        "*",
        F.sum("premium_adj").over(w_status_by_premium_running).alias("running_premium_adj"),
        F.row_number().over(w_holder_id_by_start_date).alias("rn"),
        F.lead("start_date").over(w_status_by_premium).alias("next_start_date"),
        F.max("premium_adj").over(w_status).alias("status_max_premium_adj"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.dense_rank().over(w_holder_id_by_holder_joined_o).alias("holder_joined_on_dense_rank"),
        (F.col("premium_adj") - F.lag("premium_adj").over(w_holder_segment_by_start_date)).alias("premium_adj_change"),
        F.size(F.collect_set("holder_id").over(w_status)).alias("distinct_holder_id_in_status"),
        F.max("premium_adj").over(w_holder_segment).alias("holder_segment_max_premium_adj"),
        F.lead("premium_adj", 3).over(w_status_by_start_date).alias("next3_premium_adj"),
        F.last("holder_joined_on", ignorenulls=True).over(w_status_by_start_date_running).alias("last_known_holder_joined_on"),
        F.size(F.collect_set("holder_id").over(w_holder_segment)).alias("distinct_holder_id_in_holder_segment"),
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
    )
)

first_rows = (
    metrics
    .filter(F.col("holder_joined_on_dense_rank") == 1)
    .drop("holder_joined_on_dense_rank")
)

rollup_base = (
    first_rows
    .groupBy(
        "holder_segment",
        F.date_trunc("month", "next_start_date").alias("next_start_date_month"),
    )
    .agg(
        F.sum("premium_share_of_status").alias("total_premium_share_of_status"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("holder_segment") != "RETAIL", 1).otherwise(0)).alias("n_holder_segment_retail"),
    )
)

ranked = (
    rollup_base
    .select(
        "*",
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_holder_segment), 2).alias("n_rows_vs_holder_segment_avg"),
        F.round(F.percent_rank().over(w_next_start_date_month_month), 3).alias("n_holder_segment_retail_percent_rank"),
        F.first("n_rows", ignorenulls=True).over(w_next_start_date_month_month_2).alias("first_known_n_rows"),
    )
)

result = (
    ranked
    .select(
        "holder_segment",
        "next_start_date_month",
        "total_premium_share_of_status",
        "n_rows_vs_holder_segment_avg",
        "first_known_n_rows",
        F.least(F.col("first_known_n_rows"), F.col("n_rows")).alias("least_first_known_n_rows_n_rows"),
    )
    .orderBy(
        F.col("least_first_known_n_rows_n_rows").desc_nulls_first(),
        F.col("next_start_date_month").asc_nulls_last(),
    )
)
