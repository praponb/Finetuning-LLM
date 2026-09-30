from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_rows_7d = Window.orderBy(F.col("prior_rows_7d").asc_nulls_last(), "id")
w_all_by_prior_rows_7d_2 = Window.orderBy("prior_rows_7d", "id")
w_prior_rows_7d = Window.partitionBy("prior_rows_7d")

matched = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

with_windows = (
    matched
    .select(
        "*",
        F.ntile(3).over(w_all_by_prior_rows_7d).alias("prior_rows_7d_tercile"),
        (F.col("prior_rows_7d") - F.lag("prior_rows_7d").over(w_all_by_prior_rows_7d_2)).alias("prior_rows_7d_change"),
        F.max("prior_rows_7d").over(w_prior_rows_7d).alias("prior_rows_7d_max_prior_rows_7d"),
    )
)

with_details_listener = (
    with_windows.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_7d",
        "s.prior_rows_7d_tercile",
        "s.prior_rows_7d_change",
        "s.prior_rows_7d_max_prior_rows_7d",
        F.col("l.country").alias("listener_country"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.username").alias("listener_username"),
    )
)

summary = (
    with_details_listener
    .groupBy(F.year("played_at").alias("played_at_year"))
    .agg(
        F.round(F.avg("prior_rows_7d_tercile"), 2).alias("avg_prior_rows_7d_tercile"),
        F.percentile_approx("prior_rows_7d_change", 0.5).alias("median_prior_rows_7d_change"),
        F.sum(F.when(~F.col("listener_country").isin("KR", "TH"), 1).otherwise(0)).alias("n_listener_country_not_in_kr_t"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.max("played_at").alias("last_played_at"),
        F.array_sort(F.collect_set("listener_country")).alias("listener_country_list"),
        F.avg("prior_rows_7d_change").alias("avg_prior_rows_7d_change_check"),
    )
    .filter(F.col("avg_prior_rows_7d_change_check") > -2)
    .drop("avg_prior_rows_7d_change_check")
)

result = (
    summary
    .select(
        "played_at_year",
        "n_listener_country_not_in_kr_t",
        "distinct_listener_ids",
    )
    .unpivot(
        ids=["played_at_year"],
        values=["n_listener_country_not_in_kr_t", "distinct_listener_ids"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("played_at_year", "measure")
)
