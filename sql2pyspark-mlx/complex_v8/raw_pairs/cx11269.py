from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_month_by_min = Window.partitionBy(F.date_trunc("month", "end_date_month")).orderBy(F.col("min_premium").desc(), "product", "start_date_year", "end_date_month")
w_end_date_month_month_by_min_2 = Window.partitionBy(F.date_trunc("month", "end_date_month")).orderBy(F.col("min_premium").desc(), "product", "start_date_year", "end_date_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_policys = (
    df_policies
    .filter(F.col("status") != "LAPSED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

stats = (
    filtered_policys
    .filter(F.col("premium") <= 2945.9)
    .groupBy(
        "product",
        F.year("start_date").alias("start_date_year"),
        F.date_trunc("month", "end_date").alias("end_date_month"),
    )
    .agg(
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.min("premium").alias("min_premium"),
        F.sum("premium").alias("total_premium"),
    )
    .filter(F.col("total_premium") > 1132.1)
)

with_trends = (
    stats
    .select(
        "product",
        "start_date_year",
        "end_date_month",
        "median_premium",
        F.row_number().over(w_end_date_month_month_by_min).alias("seq_no"),
        F.first("end_date_month").over(w_end_date_month_month_by_min_2).alias("first_end_date_month"),
        F.ntile(3).over(w_end_date_month_month_by_min).alias("min_premium_tercile"),
    )
)

annotated = (
    with_trends
    .select(
        "*",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.round(F.col("start_date_year") / F.nullif(F.col("median_premium"), F.lit(0)), 3).alias("start_date_year_per_median_premium"),
        F.date_trunc("year", F.col("first_end_date_month")).alias("first_end_date_month_year"),
    )
)

result = (
    annotated
    .select(
        "product",
        "start_date_year",
        "seq_no",
        "first_end_date_month",
        "min_premium_tercile",
        "first_end_date_month_year",
        F.round(F.col("seq_no") / F.nullif(F.col("product_len"), F.lit(0)), 3).alias("seq_no_per_product_len"),
    )
    .orderBy(F.col("first_end_date_month_year").desc())
)
