from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_start_date = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")
w_product_by_start_date = Window.partitionBy("product").orderBy(F.col("start_date").asc_nulls_last(), "id")
w_status = Window.partitionBy("status")
w_status_by_start_date_running = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_rank_2_by_days_si = Window.partitionBy("start_date_rank_2").orderBy("days_since_prev_holder_joined_on")
w_start_date_seq_first_known_s = Window.partitionBy("start_date_seq", F.month("first_known_start_date"))
w_all_by_start_date_percent_ra = Window.orderBy(F.col("start_date_percent_rank").asc_nulls_last())

cleaned = (
    df_policies
    .filter((F.col("product") != "LIFE") | (F.col("premium").between(1042.5, 3465.4)))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.holder_name", F.col("p.joined_on").alias("holder_joined_on"))
)

windowed = (
    combined_details
    .select(
        "*",
        F.rank().over(w_status_by_start_date).alias("start_date_rank"),
        F.round(F.percent_rank().over(w_product_by_start_date), 3).alias("start_date_percent_rank"),
        F.datediff(F.col("holder_joined_on"), F.lag("holder_joined_on").over(w_product_by_start_date)).alias("days_since_prev_holder_joined_on"),
        F.row_number().over(w_status_by_start_date).alias("start_date_seq"),
        F.rank().over(w_product_by_start_date).alias("start_date_rank_2"),
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
        F.first("start_date", ignorenulls=True).over(w_status_by_start_date_running).alias("first_known_start_date"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.round(F.percent_rank().over(w_start_date_rank_2_by_days_si), 3).alias("days_since_prev_holder_joined_on_percent_rank"),
        F.count(F.lit(1)).over(w_start_date_seq_first_known_s).alias("rows_in_start_date_seq_first_known_start_date_mo"),
        F.dense_rank().over(w_all_by_start_date_percent_ra).alias("start_date_percent_rank_dense_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("start_date_rank_2") <= 9)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "days_since_start_date",
        "holder_joined_on",
        "start_date_rank",
        "days_since_prev_holder_joined_on",
        "start_date_seq",
        "start_date_rank_2",
        "premium_vs_status_avg",
        "first_known_start_date",
        "start_date_percent_rank_dense_rank",
        F.lower(F.regexp_replace(F.trim(F.col("holder_name")), " ", "_")).alias("holder_name_slug"),
    )
    .orderBy(
        F.col("days_since_start_date").desc(),
        F.col("start_date").desc_nulls_first(),
        "id",
    )
    .limit(5)
)
