from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_start_date_year_by = Window.partitionBy("holder_id", F.year("start_date")).orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_end_date_month = Window.partitionBy("holder_id", F.date_trunc("month", "end_date"))

recent_policys = (
    df_policies
    .filter(F.col("status") == "ACTIVE")
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
    )
)

windowed = (
    recent_policys
    .select(
        "*",
        F.nth_value("end_date", 2).over(w_holder_id_start_date_year_by).alias("second_end_date"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_end_date_month), 2).alias("premium_vs_holder_id_end_date_month_avg"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_end_date_month), F.lit(0)), 4).alias("premium_share_of_holder_id_end_date_month"),
    )
)

qualified = (
    windowed.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment").isNotNull()), "left_anti")
)

enriched = (
    qualified
    .select(
        "*",
        F.dayofweek(F.col("second_end_date")).alias("second_end_date_dayofweek"),
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
        F.when(F.col("premium") >= 3521.8, "HIGH").when(F.col("premium") >= 1541.8, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

summary = (
    enriched
    .groupBy(
        F.date_trunc("month", "second_end_date").alias("second_end_date_month"),
        "holder_id",
        F.date_trunc("month", "start_date").alias("start_date_month"),
    )
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.datediff(F.max("second_end_date"), F.min("second_end_date")).alias("second_end_date_span_days"),
        F.max("premium").alias("max_premium"),
        F.min("premium_vs_holder_id_end_date_month_avg").alias("min_premium_vs_holder_id_end_date_month_avg"),
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
    )
)

tagged = (
    summary
    .select(
        "second_end_date_month",
        "holder_id",
        "start_date_month",
        "second_end_date_span_days",
        "max_premium",
        "min_premium_vs_holder_id_end_date_month_avg",
        "end_date_span_days",
        F.round(F.col("avg_premium") * 1.1, 2).alias("avg_premium_adj"),
        (F.floor(F.col("avg_premium") / 1000) * 1000).alias("avg_premium_bucket"),
    )
)

derived = (
    tagged
    .filter(F.col("max_premium") < 3080.0)
    .select(
        "second_end_date_month",
        "holder_id",
        "start_date_month",
        "second_end_date_span_days",
        "max_premium",
        "min_premium_vs_holder_id_end_date_month_avg",
        "avg_premium_adj",
        (F.col("avg_premium_bucket") + F.col("avg_premium_adj")).alias("avg_premium_bucket_plus_avg_premium_adj"),
        F.round(F.col("max_premium") * 0.9, 2).alias("max_premium_adj"),
        F.coalesce(F.col("avg_premium_adj"), F.lit(0.0)).alias("avg_premium_adj_filled"),
    )
)

result = (
    derived
    .select(
        "second_end_date_month",
        "holder_id",
        "start_date_month",
        "second_end_date_span_days",
        "min_premium_vs_holder_id_end_date_month_avg",
        "avg_premium_bucket_plus_avg_premium_adj",
        "avg_premium_adj_filled",
    )
)
