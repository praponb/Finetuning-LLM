from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year_product_by_f = Window.partitionBy(F.year("start_date"), "product").orderBy("first_policy_start_date", "id")
w_start_date_year_product_by_f_2 = Window.partitionBy(F.year("start_date"), "product").orderBy("first_policy_start_date", "id").rowsBetween(-3, -1)
w_start_date_month = Window.partitionBy(F.month("start_date"))
w_start_date_year_product_by_f_3 = Window.partitionBy(F.year("start_date"), "product").orderBy("first_policy_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_policys = (
    df_policies
    .filter((F.year(F.col("end_date")) == 2026) & (F.col("status") == "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.last_day(F.col("end_date")).alias("end_date_month_end"),
    )
)

with_activity = (
    scoped_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date").between(F.lit("2025-12-05").cast("date"), F.lit("2027-05-16").cast("date"))) & (F.col("p.premium") >= 1132.1), "left_semi")
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("end_date").alias("last_policy_end_date"),
        F.min("start_date").alias("first_policy_start_date"),
    )
)

profiled = (
    with_activity.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.last_policy_end_date", "p.first_policy_start_date")
)

sequenced = (
    profiled
    .select(
        "*",
        F.lag("end_date_month_end", 2).over(w_start_date_year_product_by_f).alias("prev2_end_date_month_end"),
        F.round(F.avg("premium").over(w_start_date_year_product_by_f_2), 2).alias("premium_avg_3_prior"),
        F.round(F.col("premium") - F.avg("premium").over(w_start_date_month), 2).alias("premium_vs_start_date_month_avg"),
        F.last("premium").over(w_start_date_year_product_by_f_3).alias("last_premium"),
    )
)

result = (
    sequenced
    .filter(F.upper(F.trim(F.col("status_clean"))).like("C%"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status_clean",
        "last_policy_end_date",
        "first_policy_start_date",
        "premium_avg_3_prior",
        "premium_vs_start_date_month_avg",
        "last_premium",
        F.last_day(F.col("end_date_month_end")).alias("end_date_month_end_month_end"),
    )
    .orderBy(
        F.col("premium").asc(),
        F.col("first_policy_start_date").asc_nulls_last(),
    )
)
