from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year_by_start_date = Window.partitionBy(F.year("end_date")).orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status = Window.partitionBy("status")
w_end_date_year_by_start_date_2 = Window.partitionBy(F.year("end_date")).orderBy("start_date", "id")
w_all_by_start_date = Window.orderBy("start_date", "id")

recent_policys = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-05-28").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
    )
)

with_windows = (
    recent_policys
    .select(
        "*",
        F.max("premium").over(w_end_date_year_by_start_date).alias("max_premium_so_far"),
        F.sum("premium").over(w_status).alias("status_sum_premium"),
        F.row_number().over(w_end_date_year_by_start_date_2).alias("rn"),
        F.lag("end_date", 3).over(w_all_by_start_date).alias("prev3_end_date"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["rn", "status"], ["rn"]],
        "rn", "status",
    )
    .agg(F.grouping_id().alias("gid"), F.min("premium").alias("min_premium"))
)
