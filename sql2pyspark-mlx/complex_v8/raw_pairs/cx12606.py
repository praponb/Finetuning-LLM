from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_start_date_ties = Window.orderBy("start_date")

prepared_policys = (
    df_policies
    .filter(F.col("end_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(" / ", F.col("status"), F.col("product")).alias("status_product_label"),
    )
)

qualified = (
    prepared_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product").isNotNull()) & (F.col("p.start_date") >= F.lit("2024-06-19").cast("date")), "left_anti")
    .filter(F.year(F.col("end_date")) == 2026)
)

metrics = (
    qualified
    .select(
        "*",
        F.row_number().over(w_all_by_start_date).alias("seq_no"),
        F.round(F.percent_rank().over(w_all_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_all_by_start_date)).alias("days_since_prev_end_date"),
        F.lag("start_date", 2).over(w_all_by_start_date).alias("prev2_start_date"),
        F.ntile(4).over(w_all_by_start_date).alias("start_date_quartile"),
        F.round(F.cume_dist().over(w_all_by_start_date), 3).alias("start_date_cume_dist"),
    )
)

summary = (
    metrics
    .groupBy(
        "seq_no",
        F.date_trunc("month", "prev2_start_date").alias("prev2_start_date_month"),
        "status",
    )
    .agg(
        F.min("start_date").alias("first_start_date"),
        F.sum("start_date_quartile").alias("total_start_date_quartile"),
    )
)

without_matches = (
    summary
    .crossJoin(summary.agg(F.min("seq_no").alias("overall_min_seq_no")))
    .filter(F.col("seq_no") > F.col("overall_min_seq_no"))
    .drop("overall_min_seq_no")
)

result = (
    without_matches
    .select(
        "seq_no",
        "prev2_start_date_month",
        "status",
        "first_start_date",
        "total_start_date_quartile",
        F.add_months(F.col("first_start_date"), 6).alias("first_start_date_plus_6m"),
    )
    .orderBy(F.col("first_start_date").desc(), F.col("status").asc())
)
