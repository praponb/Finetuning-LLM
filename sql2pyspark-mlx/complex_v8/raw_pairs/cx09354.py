from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date_full = Window.orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date = Window.orderBy(F.col("end_date").asc_nulls_last(), "id")
w_all_by_start_date_running = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_row_count = Window.orderBy(F.col("row_count").asc_nulls_last(), "product", "prev_start_date_month", "holder_id")
w_row_count = Window.partitionBy("row_count")
w_holder_id = Window.partitionBy("holder_id")
w_all_by_start_date_span_days = Window.orderBy("start_date_span_days", "product", "prev_start_date_month", "holder_id")
w_all_by_start_date_span_days_2 = Window.orderBy("start_date_span_days", "product", "prev_start_date_month", "holder_id").rowsBetween(-2, -1)
w_all_by_start_date_span_days_3 = Window.orderBy("start_date_span_days", "product", "prev_start_date_month", "holder_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_span_days_4 = Window.orderBy("start_date_span_days", "product", "prev_start_date_month", "holder_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_policys = (
    df_policies
    .filter((F.col("product") == "AUTO") & (F.col("holder_id") >= 2))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

qualified = (
    scoped_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") >= 1042.5) & (F.col("p.end_date") >= F.lit("2025-10-12").cast("date")), "left_anti")
    .filter(F.col("end_date") >= F.lit("2026-01-23").cast("date"))
)

metrics = (
    qualified
    .select(
        "*",
        F.last("end_date").over(w_all_by_end_date_full).alias("last_end_date"),
        F.lag("start_date").over(w_all_by_end_date).alias("prev_start_date"),
        F.round(F.cume_dist().over(w_all_by_end_date), 3).alias("end_date_cume_dist"),
        F.first("end_date", ignorenulls=True).over(w_all_by_start_date_running).alias("first_known_end_date"),
    )
)

totals = (
    metrics
    .filter(F.col("end_date_cume_dist") < 0.8)
    .groupBy(
        "product",
        F.date_trunc("month", "prev_start_date").alias("prev_start_date_month"),
        "holder_id",
    )
    .agg(
        F.datediff(F.max("start_date"), F.min("start_date")).alias("start_date_span_days"),
        F.sum("end_date_cume_dist").alias("total_end_date_cume_dist"),
        F.count("*").alias("row_count"),
        F.datediff(F.max("prev_start_date"), F.min("prev_start_date")).alias("prev_start_date_span_days"),
        F.avg("end_date_cume_dist").alias("avg_end_date_cume_dist_check"),
    )
    .filter(F.col("avg_end_date_cume_dist_check") > 0.1)
    .drop("avg_end_date_cume_dist_check")
)

sequenced = (
    totals
    .select(
        "product",
        "prev_start_date_month",
        "holder_id",
        "start_date_span_days",
        "total_end_date_cume_dist",
        "row_count",
        F.lead("total_end_date_cume_dist", 2).over(w_all_by_row_count).alias("next2_total_end_date_cume_dist"),
        F.round(F.col("row_count") - F.avg("row_count").over(w_row_count), 2).alias("row_count_vs_row_count_avg"),
    )
)

ranked = (
    sequenced
    .select(
        "*",
        F.max("total_end_date_cume_dist").over(w_holder_id).alias("holder_id_max_total_end_date_cume_dist"),
        F.ntile(5).over(w_all_by_start_date_span_days).alias("start_date_span_days_quintile"),
        F.max("row_count").over(w_all_by_start_date_span_days_2).alias("row_count_max_2_prior"),
        F.round(F.col("row_count_vs_row_count_avg") / F.nullif(F.sum("row_count_vs_row_count_avg").over(w_holder_id), F.lit(0)), 4).alias("row_count_vs_row_count_avg_share_of_holder_id"),
        F.nth_value("prev_start_date_month", 2).over(w_all_by_start_date_span_days_3).alias("second_prev_start_date_month"),
        F.last("start_date_span_days", ignorenulls=True).over(w_all_by_start_date_span_days_4).alias("last_known_start_date_span_days"),
    )
)

result = (
    ranked
    .select(
        "product",
        "prev_start_date_month",
        "next2_total_end_date_cume_dist",
        "row_count_vs_row_count_avg_share_of_holder_id",
    )
    .unpivot(
        ids=["product", "prev_start_date_month"],
        values=["next2_total_end_date_cume_dist", "row_count_vs_row_count_avg_share_of_holder_id"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
