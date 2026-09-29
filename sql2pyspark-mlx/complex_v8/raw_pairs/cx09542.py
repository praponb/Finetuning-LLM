from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_days_since_start_d = Window.partitionBy("status").orderBy(F.col("days_since_start_date").desc(), "id")
w_start_date_year_by_days_sinc = Window.partitionBy(F.year("start_date")).orderBy(F.col("days_since_start_date").desc(), "id")
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_seq_no_start_date_month_by_s = Window.partitionBy("seq_no", F.month("start_date")).orderBy(F.col("seq_no").desc_nulls_first(), "id")
w_seq_no_start_date_month_by_s_2 = Window.partitionBy("seq_no", F.month("start_date")).orderBy(F.col("seq_no").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_status = Window.partitionBy("status")

base_policys = (
    df_policies
    .filter((F.col("holder_id") >= 6) & (F.col("end_date") < F.lit("2026-08-31").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 2945.9).alias("premium_distance"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

sequenced = (
    base_policys
    .select(
        "*",
        F.lead("start_date", 2).over(w_status_by_days_since_start_d).alias("next2_start_date"),
        F.lag("premium", 1, 0).over(w_start_date_year_by_days_sinc).alias("prev_premium"),
        F.row_number().over(w_holder_id_by_end_date).alias("seq_no"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.ntile(10).over(w_seq_no_start_date_month_by_s).alias("seq_no_decile"),
        F.sum("days_since_start_date").over(w_seq_no_start_date_month_by_s_2).alias("days_since_start_date_sum_2"),
        F.round(F.avg("prev_premium").over(w_seq_no_start_date_month_by_s_2), 2).alias("prev_premium_avg_2"),
        F.size(F.collect_set("product").over(w_status)).alias("distinct_product_in_status"),
    )
)

result = (
    metrics
    .select(
        "id",
        "days_since_start_date",
        "distinct_product_in_status",
        "seq_no_decile",
        "days_since_start_date_sum_2",
    )
    .unpivot(
        ids=["id"],
        values=["days_since_start_date", "distinct_product_in_status", "seq_no_decile", "days_since_start_date_sum_2"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
)
