from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_end_date_mo = Window.partitionBy(F.date_trunc("month", "start_date"), F.month("end_date")).orderBy(F.col("end_date").desc(), "id")
w_start_date_month_end_date_mo_2 = Window.partitionBy(F.date_trunc("month", "start_date"), F.month("end_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_ties = Window.orderBy("start_date")
w_all_by_start_date = Window.orderBy("start_date", "id")

filtered = (
    df_policies
    .filter(F.col("holder_id") >= 19)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_start_date_month_end_date_mo)).alias("days_since_prev_start_date"),
        F.first("end_date").over(w_start_date_month_end_date_mo_2).alias("first_end_date"),
        F.rank().over(w_all_by_start_date_ties).alias("start_date_rank"),
        F.ntile(3).over(w_start_date_month_end_date_mo).alias("end_date_tercile"),
        F.lag("end_date").over(w_all_by_start_date).alias("prev_end_date"),
        F.nth_value("start_date", 2).over(w_start_date_month_end_date_mo_2).alias("second_start_date"),
        F.row_number().over(w_start_date_month_end_date_mo).alias("end_date_seq"),
        F.dense_rank().over(w_start_date_month_end_date_mo).alias("end_date_dense_rank"),
        F.row_number().over(w_all_by_start_date).alias("start_date_seq"),
        F.lag("start_date", 3).over(w_start_date_month_end_date_mo).alias("prev3_start_date"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_start_date_month_end_date_mo)).alias("days_since_prev_end_date"),
        F.nth_value("end_date", 2).over(w_start_date_month_end_date_mo_2).alias("second_end_date"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "first_end_date",
        "start_date_rank",
        "end_date_tercile",
        "prev_end_date",
        "second_start_date",
        "start_date_seq",
        "prev3_start_date",
        "second_end_date",
    )
    .orderBy("start_date_seq", F.col("prev_end_date").desc())
)
