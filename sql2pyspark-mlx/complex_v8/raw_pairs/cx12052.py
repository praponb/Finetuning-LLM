from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year_holder_id = Window.partitionBy(F.year("start_date"), "holder_id")
w_holder_id_by_start_date_ties = Window.partitionBy("holder_id").orderBy("start_date")
w_holder_id_by_start_date_movi = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)

scoped = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.count(F.lit(1)).over(w_start_date_year_holder_id).alias("rows_in_start_date_year_holder_id"),
        F.round(F.avg("premium").over(w_start_date_year_holder_id), 2).alias("start_date_year_holder_id_avg_premium"),
        F.round(F.percent_rank().over(w_holder_id_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.max("premium").over(w_holder_id_by_start_date_movi).alias("premium_max_7"),
    )
)

result = (
    with_windows
    .rollup("status", "holder_id")
    .agg(
        F.round(F.stddev("start_date_percent_rank"), 2).alias("stddev_start_date_percent_rank"),
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("holder_id").asc_nulls_last())
)
