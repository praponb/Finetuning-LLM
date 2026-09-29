from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_by_end_date = Window.partitionBy(F.month("start_date")).orderBy(F.col("end_date").asc_nulls_last(), "id")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_product_by_start_date = Window.partitionBy("product").orderBy(F.col("start_date").desc_nulls_first(), "id")
w_holder_id_status_by_start_da = Window.partitionBy("holder_id", "status").orderBy("start_date", "id")
w_status_holder_id_by_start_da = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_by_end_date = Window.partitionBy("product").orderBy("end_date", "id")

filtered_policys = (
    df_policies
    .filter((~F.col("product").isin("TRAVEL", "AUTO")) & (F.col("start_date") >= F.lit("2024-07-19").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

with_trends = (
    filtered_policys
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        F.ntile(3).over(w_start_date_month_by_end_date).alias("end_date_tercile"),
        F.ntile(5).over(w_all_by_end_date).alias("end_date_quintile"),
        F.lag("end_date").over(w_all_by_end_date).alias("prev_end_date"),
        F.row_number().over(w_product_by_start_date).alias("rn"),
        F.row_number().over(w_start_date_month_by_end_date).alias("rn_2"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_status_by_start_da)).alias("days_since_prev_start_date"),
        F.last("end_date").over(w_status_holder_id_by_start_da).alias("last_end_date"),
        F.round(F.cume_dist().over(w_product_by_end_date), 3).alias("end_date_cume_dist"),
        F.lead("start_date", 3).over(w_all_by_end_date).alias("next3_start_date"),
        F.lead("start_date", 3).over(w_start_date_month_by_end_date).alias("next3_start_date_2"),
    )
)

annotated = (
    with_trends
    .select(
        "*",
        F.date_format(F.col("start_date"), "MMM yyyy").alias("start_date_label"),
        (F.col("end_date_quintile") + F.col("rn_2")).alias("end_date_quintile_plus_rn_2"),
    )
)

result = (
    annotated
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "end_date_quintile",
        "prev_end_date",
        "rn",
        "rn_2",
        "days_since_prev_start_date",
        "last_end_date",
        "end_date_cume_dist",
        "start_date_label",
        "end_date_quintile_plus_rn_2",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
    .orderBy(F.col("status").asc(), "id")
    .limit(10)
)
