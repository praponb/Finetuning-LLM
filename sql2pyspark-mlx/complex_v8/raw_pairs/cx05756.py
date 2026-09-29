from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_end_date_full = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_end_date = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")
w_status_by_end_date_running = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_end_date_ties = Window.partitionBy("status").orderBy(F.col("end_date").desc())
w_status_by_next_end_date_ties = Window.partitionBy("status").orderBy(F.col("next_end_date").desc())
w_holder_id_by_end_date_full = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_policies
    .filter(F.col("status").isin("CANCELLED", "EXPIRED", "LAPSED"))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.nth_value("start_date", 2).over(w_status_by_end_date_full).alias("second_start_date"),
        F.lead("end_date").over(w_status_by_end_date).alias("next_end_date"),
        F.last("end_date", ignorenulls=True).over(w_status_by_end_date_running).alias("last_known_end_date"),
        F.round(F.percent_rank().over(w_status_by_end_date_ties), 3).alias("end_date_percent_rank"),
    )
)

ranked = (
    windowed
    .withColumn("next_end_date_percent_rank", F.round(F.percent_rank().over(w_status_by_next_end_date_ties), 3))
    .withColumn("last_end_date", F.last("end_date").over(w_holder_id_by_end_date_full))
)

result = (
    ranked
    .rollup("holder_id", "status")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("next_end_date_percent_rank"), 2).alias("total_next_end_date_percent_rank"),
        F.min("end_date_percent_rank").alias("min_end_date_percent_rank"),
    )
    .orderBy(F.col("holder_id").asc_nulls_last(), F.col("status").asc_nulls_last())
)
