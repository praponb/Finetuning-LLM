from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_status = Window.partitionBy("start_date_month", "status")

filtered_policys = (
    df_policies
    .filter((~F.col("product").isin("LIFE", "TRAVEL")) & (F.col("status") == "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.trunc(F.col("end_date"), "MM").alias("end_date_month_start"),
        F.month(F.col("start_date")).alias("start_date_month"),
    )
)

screened = (
    filtered_policys
    .crossJoin(filtered_policys.agg(F.max("start_date_month").alias("overall_max_start_date_month")))
    .filter(F.col("start_date_month") < F.col("overall_max_start_date_month"))
    .drop("overall_max_start_date_month")
)

with_windows = (
    screened
    .select(
        "id",
        "premium",
        "start_date",
        "status",
        "end_date_month_start",
        "start_date_month",
        F.size(F.collect_set("holder_id").over(w_start_date_month_status)).alias("distinct_holder_id_in_start_date_month_status"),
        F.round(F.col("start_date_month") - F.avg("start_date_month").over(w_start_date_month_status), 2).alias("start_date_month_vs_start_date_month_status_avg"),
        F.count(F.lit(1)).over(w_start_date_month_status).alias("rows_in_start_date_month_status"),
    )
)

result = (
    with_windows
    .select("id", "start_date_month", "distinct_holder_id_in_start_date_month_status")
    .unpivot(
        ids=["id"],
        values=["start_date_month", "distinct_holder_id_in_start_date_month_status"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
