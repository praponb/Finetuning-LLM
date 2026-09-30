from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_start_date_quarter = Window.partitionBy("holder_id", F.month("start_date_quarter"))
w_start_date_year_by_end_date = Window.partitionBy(F.year("start_date")).orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_plus_1m_month = Window.partitionBy(F.date_trunc("month", "end_date_plus_1m"))
w_end_date_year_by_premium_fil = Window.partitionBy(F.year("end_date")).orderBy("premium_filled_vs_holder_id_start_date_quarter_m", "id")
w_latest = Window.partitionBy("holder_id").orderBy("end_date_plus_1m_month_avg_premium_filled", "id")

valid_policys = (
    df_policies
    .filter(F.col("product").isin("LIFE", "TRAVEL", "AUTO"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

metrics = (
    valid_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "start_date_quarter",
        "premium_filled",
        F.round(F.col("premium_filled") / F.nullif(F.sum("premium_filled").over(w_holder_id_start_date_quarter), F.lit(0)), 4).alias("premium_filled_share_of_holder_id_start_date_qua"),
        F.round(F.col("premium_filled") - F.avg("premium_filled").over(w_holder_id_start_date_quarter), 2).alias("premium_filled_vs_holder_id_start_date_quarter_m"),
        F.first("end_date").over(w_start_date_year_by_end_date).alias("first_end_date"),
        F.count(F.lit(1)).over(w_holder_id_start_date_quarter).alias("rows_in_holder_id_start_date_quarter_month"),
    )
)

tagged = (
    metrics
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "premium_filled",
        "premium_filled_vs_holder_id_start_date_quarter_m",
        "first_end_date",
        F.add_months(F.col("end_date"), 1).alias("end_date_plus_1m"),
    )
)

with_trends = (
    tagged
    .select(
        "id",
        "holder_id",
        "premium_filled_vs_holder_id_start_date_quarter_m",
        "first_end_date",
        F.count(F.lit(1)).over(w_end_date_plus_1m_month).alias("rows_in_end_date_plus_1m_month"),
        F.round(F.avg("premium_filled").over(w_end_date_plus_1m_month), 2).alias("end_date_plus_1m_month_avg_premium_filled"),
        F.datediff(F.col("end_date_plus_1m"), F.lag("end_date_plus_1m").over(w_end_date_year_by_premium_fil)).alias("days_since_prev_end_date_plus_1m"),
        (F.col("premium") - F.lag("premium").over(w_end_date_year_by_premium_fil)).alias("premium_change"),
        F.dense_rank().over(w_end_date_year_by_premium_fil).alias("premium_filled_vs_holder_id_start_date_quarter_m_2"),
    )
)

unique_rows = (
    with_trends
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

holder_id_stats = (
    unique_rows
    .groupBy("holder_id")
    .agg(F.percentile_approx("premium_change", 0.5).alias("median_premium_change"))
)

compared = (
    unique_rows.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium_change") >= F.col("g.median_premium_change"))
    .select(
        "s.*",
        "g.median_premium_change",
        F.round(F.col("s.premium_change") - F.col("g.median_premium_change"), 2).alias("premium_change_minus_median"),
    )
)

with_details = (
    compared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium_filled_vs_holder_id_start_date_quarter_m",
        "s.first_end_date",
        "s.rows_in_end_date_plus_1m_month",
        "s.end_date_plus_1m_month_avg_premium_filled",
        "s.premium_change",
        "s.premium_filled_vs_holder_id_start_date_quarter_m_2",
        "s.premium_change_minus_median",
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    with_details
    .select("id", "first_end_date", "premium_change_minus_median")
    .distinct()
    .orderBy(F.col("premium_change_minus_median").asc())
)
