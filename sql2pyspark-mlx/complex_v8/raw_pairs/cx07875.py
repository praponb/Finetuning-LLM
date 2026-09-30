from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy("start_date", "id")
w_start_date_year_by_start_dat = Window.partitionBy(F.year("start_date")).orderBy(F.col("start_date").desc(), "id")
w_holder_id_by_start_date_ties = Window.partitionBy("holder_id").orderBy("start_date")
w_holder_id_start_date_year_by = Window.partitionBy("holder_id", F.year("start_date")).orderBy(F.col("start_date").desc(), "id")
w_holder_id_start_date_year_by_2 = Window.partitionBy("holder_id", F.year("start_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_start_date_full = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_dense_rank_2 = Window.partitionBy("start_date_dense_rank_2")
w_prev_start_date_month = Window.partitionBy(F.date_trunc("month", "prev_start_date"))
w_all_by_next3_start_date = Window.orderBy("next3_start_date", "id")

prepared_policys = (
    df_policies
    .filter((F.col("premium").between(918.1, 4688.7)) & (F.year(F.col("start_date")) == 2025))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

with_details = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") < 0.6), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        F.col("p.segment").alias("holder_segment"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.ntile(4).over(w_holder_id_by_start_date).alias("start_date_quartile"),
        F.round(F.percent_rank().over(w_start_date_year_by_start_dat), 3).alias("start_date_percent_rank"),
        F.dense_rank().over(w_holder_id_by_start_date_ties).alias("start_date_dense_rank"),
        F.lag("start_date").over(w_holder_id_start_date_year_by).alias("prev_start_date"),
        F.lead("start_date").over(w_start_date_year_by_start_dat).alias("next_start_date"),
        F.round(F.percent_rank().over(w_holder_id_start_date_year_by), 3).alias("start_date_percent_rank_2"),
        F.first("start_date", ignorenulls=True).over(w_holder_id_start_date_year_by_2).alias("first_known_start_date"),
        F.lead("start_date", 3).over(w_holder_id_start_date_year_by).alias("next3_start_date"),
        F.lag("start_date").over(w_start_date_year_by_start_dat).alias("prev_start_date_2"),
        F.dense_rank().over(w_holder_id_by_start_date).alias("start_date_dense_rank_2"),
        F.nth_value("start_date", 2).over(w_holder_id_by_start_date_full).alias("second_start_date"),
    )
)

first_rows = (
    windowed
    .filter((F.col("start_date_dense_rank_2") == 1) & (F.col("start_date_percent_rank") <= 0.6))
)

metrics = (
    first_rows
    .select(
        "id",
        "holder_id",
        "holder_segment",
        "start_date_percent_rank",
        "prev_start_date",
        "next_start_date",
        "first_known_start_date",
        "next3_start_date",
        "prev_start_date_2",
        "start_date_dense_rank_2",
        "second_start_date",
        F.round(F.avg("start_date_dense_rank").over(w_start_date_dense_rank_2), 2).alias("start_date_dense_rank_2_avg_start_date_dense_ran"),
        F.round(F.avg("start_date_dense_rank").over(w_prev_start_date_month), 2).alias("prev_start_date_month_avg_start_date_dense_rank"),
        F.lead("second_start_date").over(w_all_by_next3_start_date).alias("next_second_start_date"),
    )
)

result = (
    metrics
    .select(
        "id",
        "holder_id",
        "holder_segment",
        "start_date_percent_rank",
        "prev_start_date",
        "first_known_start_date",
        "next3_start_date",
        "prev_start_date_2",
        "start_date_dense_rank_2",
        "second_start_date",
        "start_date_dense_rank_2_avg_start_date_dense_ran",
        "prev_start_date_month_avg_start_date_dense_rank",
    )
    .orderBy("holder_segment", "holder_id")
)
