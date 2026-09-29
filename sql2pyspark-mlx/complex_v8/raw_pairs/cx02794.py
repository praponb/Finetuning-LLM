from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_id_by_pri = Window.partitionBy(F.month("read_at"), "site_id").orderBy(F.col("prior_kwh_1d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_site_id_by_pri_2 = Window.partitionBy(F.month("read_at"), "site_id").orderBy(F.col("prior_kwh_1d").desc(), "id").rowsBetween(-1, -1)
w_prior_rows_1d_by_first_known = Window.partitionBy("prior_rows_1d").orderBy("first_known_prior_kwh_1d", "id").rowsBetween(-6, -1)
w_prior_rows_1d_read_at_year = Window.partitionBy("prior_rows_1d", F.year("read_at"))
w_prior_rows_1d_by_first_known_2 = Window.partitionBy("prior_rows_1d").orderBy("first_known_prior_kwh_1d", "id")
w_prior_rows_1d_by_first_known_3 = Window.partitionBy("prior_rows_1d").orderBy("first_known_prior_kwh_1d")

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.kwh").alias("prior_kwh_1d"),
    )
)

windowed = (
    matched
    .select(
        "id",
        "read_at",
        "prior_rows_1d",
        F.first("prior_kwh_1d", ignorenulls=True).over(w_read_at_month_site_id_by_pri).alias("first_known_prior_kwh_1d"),
        F.sum("prior_kwh_1d").over(w_read_at_month_site_id_by_pri_2).alias("prior_kwh_1d_sum_1_prior"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "read_at",
        "prior_rows_1d",
        "prior_kwh_1d_sum_1_prior",
        F.max("prior_rows_1d").over(w_prior_rows_1d_by_first_known).alias("prior_rows_1d_max_6_prior"),
        F.row_number().over(w_prior_rows_1d_by_first_known_2).alias("seq_no"),
        F.round(F.percent_rank().over(w_prior_rows_1d_by_first_known_3), 3).alias("first_known_prior_kwh_1d_percent_rank"),
        F.dense_rank().over(w_prior_rows_1d_by_first_known_2).alias("first_known_prior_kwh_1d_dense_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("prior_rows_1d_max_6_prior") < 17)
    .select(
        "id",
        "read_at",
        "prior_rows_1d",
        "prior_kwh_1d_sum_1_prior",
        "seq_no",
        "first_known_prior_kwh_1d_percent_rank",
        "first_known_prior_kwh_1d_dense_rank",
    )
    .orderBy(F.col("first_known_prior_kwh_1d_dense_rank").desc())
)
