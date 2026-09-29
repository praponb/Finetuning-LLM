from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_holder_id_b = Window.partitionBy("start_date_month", "holder_id").orderBy(F.col("premium").desc(), "id")
w_status_len = Window.partitionBy("status_len")

valid_policys = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2026-01-24").cast("date")) & (F.col("start_date") < F.lit("2025-03-06").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.date_format(F.col("end_date"), "MMM yyyy").alias("end_date_label"),
        F.month(F.col("start_date")).alias("start_date_month"),
    )
)

tagged = (
    valid_policys
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.lower(F.trim(F.col("end_date_label"))).alias("end_date_label_clean"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.lag("status_len", 1, 0).over(w_start_date_month_holder_id_b).alias("prev_status_len"),
        F.count(F.lit(1)).over(w_status_len).alias("rows_in_status_len"),
    )
)

rollup_base = (
    windowed
    .groupBy(F.date_trunc("month", "end_date").alias("end_date_month"))
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("rows_in_status_len"))).alias("rows_in_status_len_status_expired"),
        F.count("*").alias("cnt"),
        F.sum("premium").alias("total_premium"),
        F.percentile_approx("rows_in_status_len", 0.5).alias("median_rows_in_status_len"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(~F.col("status").isin("ACTIVE", "EXPIRED"), 1).otherwise(0)).alias("n_status_not_in_active_expired"),
    )
)

labeled = (
    rollup_base
    .select(
        "end_date_month",
        "rows_in_status_len_status_expired",
        "cnt",
        F.least(F.col("cnt"), F.col("distinct_ids")).alias("least_cnt_distinct_ids"),
        F.date_trunc("week", F.col("end_date_month")).alias("end_date_month_week"),
    )
)

result = (
    labeled
    .select("end_date_month", "cnt", "least_cnt_distinct_ids")
    .unpivot(
        ids=["end_date_month"],
        values=["cnt", "least_cnt_distinct_ids"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
