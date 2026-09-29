from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_n_rows_by_min_premium_runnin = Window.partitionBy("n_rows").orderBy(F.col("min_premium").desc(), "start_date_month", "end_date_month", "holder_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_by_min_premium_prior = Window.partitionBy("n_rows").orderBy(F.col("min_premium").desc(), "start_date_month", "end_date_month", "holder_id").rowsBetween(-6, -1)

base = (
    df_policies
    .filter((F.col("end_date").isNotNull()) & (F.col("product") != "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

per_group = (
    base
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        F.date_trunc("month", "end_date").alias("end_date_month"),
        "holder_id",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.min("premium").alias("min_premium"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
    )
)

with_parent_holder = (
    per_group.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.start_date_month",
        "s.end_date_month",
        "s.holder_id",
        "s.n_rows",
        "s.min_premium",
        "s.avg_premium",
        F.col("p.region").alias("holder_region"),
    )
)

qualified = (
    with_parent_holder.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product").isNotNull()) & (F.year(F.col("p.start_date")) == 2024), "left_anti")
)

windowed = (
    qualified
    .select(
        "start_date_month",
        "end_date_month",
        "holder_id",
        "min_premium",
        "holder_region",
        F.sum("avg_premium").over(w_holder_id).alias("holder_id_sum_avg_premium"),
        F.first("end_date_month", ignorenulls=True).over(w_n_rows_by_min_premium_runnin).alias("first_known_end_date_month"),
        F.sum("n_rows").over(w_n_rows_by_min_premium_prior).alias("n_rows_sum_6_prior"),
    )
)

result = (
    windowed
    .filter(F.col("holder_id_sum_avg_premium") > 12650.0)
    .select(
        "start_date_month",
        "end_date_month",
        "holder_id",
        "min_premium",
        "holder_region",
        "holder_id_sum_avg_premium",
        "first_known_end_date_month",
        "n_rows_sum_6_prior",
    )
    .orderBy(F.col("end_date_month").asc_nulls_last())
)
