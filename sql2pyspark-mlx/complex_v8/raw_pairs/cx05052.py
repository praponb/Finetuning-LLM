from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product", "holder_id").orderBy(F.col("premium").desc(), F.col("id").desc())
w_start_date_month_product_by = Window.partitionBy(F.month("start_date"), "product").orderBy(F.col("premium").desc(), "id")
w_start_date_month_product_by_2 = Window.partitionBy(F.month("start_date"), "product").orderBy(F.col("premium").desc(), "id").rowsBetween(-6, Window.currentRow)
w_start_date_month_product_by_3 = Window.partitionBy(F.month("start_date"), "product").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_premium_quartile_by_first_st = Window.partitionBy("premium_quartile").orderBy(F.col("first_start_date").desc(), "premium_quartile", "start_date_ym")
w_premium_quartile = Window.partitionBy("premium_quartile")

base = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.date_format(F.col("start_date"), "MMM yyyy").alias("start_date_label"),
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

labeled = (
    deduped
    .filter(F.col("holder_id") >= 19)
    .withColumn("premium_distance", F.abs(F.col("premium") - 2096.6))
)

windowed = (
    labeled
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "premium_distance",
        F.ntile(4).over(w_start_date_month_product_by).alias("premium_quartile"),
        F.row_number().over(w_start_date_month_product_by).alias("premium_seq"),
        F.max("premium_adj").over(w_start_date_month_product_by_2).alias("premium_adj_max_7"),
        F.nth_value("premium", 2).over(w_start_date_month_product_by_3).alias("second_premium"),
        F.ntile(5).over(w_start_date_month_product_by).alias("premium_quintile"),
    )
)

totals = (
    windowed
    .groupBy(
        "premium_quartile",
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
    )
    .agg(
        F.min("start_date").alias("first_start_date"),
        F.min("second_premium").alias("min_second_premium"),
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.round(F.sum("premium") / F.nullif(F.sum("premium_adj_max_7"), F.lit(0)), 3).alias("premium_to_premium_adj_max_7_ratio"),
        F.datediff(F.max("start_date"), F.min("start_date")).alias("start_date_span_days"),
        F.max("start_date").alias("last_start_date"),
        F.avg("premium_quartile").alias("avg_premium_quartile_check"),
    )
    .filter(F.col("avg_premium_quartile_check") > 4)
    .drop("avg_premium_quartile_check")
)

with_windows = (
    totals
    .select(
        "premium_quartile",
        "start_date_ym",
        "first_start_date",
        "median_premium",
        "premium_to_premium_adj_max_7_ratio",
        "last_start_date",
        (F.col("start_date_span_days") - F.lag("start_date_span_days").over(w_premium_quartile_by_first_st)).alias("start_date_span_days_change"),
        F.sum("min_second_premium").over(w_premium_quartile).alias("premium_quartile_sum_min_second_premium"),
    )
)

filtered = (
    with_windows
    .crossJoin(with_windows.agg(F.avg("median_premium").alias("overall_avg_median_premium")))
    .filter(F.col("median_premium") > F.col("overall_avg_median_premium"))
    .drop("overall_avg_median_premium")
)

result = (
    filtered
    .filter(F.col("start_date_span_days_change").between(-280, 350))
    .select(
        "premium_quartile",
        "median_premium",
        "last_start_date",
        "start_date_span_days_change",
        "premium_quartile_sum_min_second_premium",
        F.date_trunc("year", F.col("last_start_date")).alias("last_start_date_year"),
    )
    .orderBy("median_premium", "premium_quartile")
)
