from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc())
w_all_by_start_date_full = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")

scoped = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
    )
)

enriched = (
    scoped
    .select(
        "*",
        F.date_format(F.col("start_date"), "MMM yyyy").alias("start_date_label"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") >= 0.3), "left_semi")
)

with_windows = (
    without_matches
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.nth_value("start_date", 2).over(w_all_by_start_date_full).alias("second_start_date"),
        F.ntile(4).over(w_all_by_start_date).alias("start_date_quartile"),
        F.lag("start_date").over(w_all_by_start_date).alias("prev_start_date"),
        F.first("start_date").over(w_all_by_start_date_full).alias("first_start_date"),
    )
)

result = (
    with_windows
    .groupBy("holder_id", "start_date")
    .pivot("status", ["CANCELLED", "EXPIRED"])
    .agg(F.max("start_date_quartile"))
    .orderBy("holder_id", "start_date")
)
