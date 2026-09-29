from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_payments
    .filter(F.to_date("paid_at") >= "2023-04-21")
    .select(
        "*",
        F.max("amount").over(w_grp).alias("running_max_amount"),
        F.avg("amount").over(w_grp).alias("running_avg_amount"),
        F.count("amount").over(w_grp).alias("running_count_amount"),
    )
)

result = (
    with_windows
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .groupingSets(
        [["activity_month", "year_start"], ["activity_month"], []],
        "activity_month",
        "year_start",
    )
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.percentile_approx("running_avg_amount", [0.25, 0.5, 0.75]).alias("running_avg_amount_quartiles"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("amount_stddev") >= 0)
    .orderBy(F.col("year_start").asc_nulls_last())
)
