from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_by_holder_r = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("holder_risk_score").desc())
w_start_date_month_by_holder_r_2 = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("holder_risk_score").desc(), "id")
w_all_by_start_date = Window.orderBy("start_date", "id")

base = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-03-06").cast("date")) & (F.col("end_date") >= F.lit("2026-03-13").cast("date")))
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

enriched_holder = (
    base.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_windows = (
    enriched_holder
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.round(F.percent_rank().over(w_start_date_month_by_holder_r), 3).alias("holder_risk_score_percent_rank"),
        F.row_number().over(w_start_date_month_by_holder_r_2).alias("seq_no"),
        F.lag("premium").over(w_all_by_start_date).alias("prev_premium"),
    )
)

result = (
    with_windows
    .filter(F.col("premium") <= 3544.1)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "holder_risk_score_percent_rank",
        "seq_no",
    )
    .orderBy("start_date")
)
