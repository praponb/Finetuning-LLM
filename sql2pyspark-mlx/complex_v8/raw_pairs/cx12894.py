from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium = Window.orderBy("premium", "id")
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id")
w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_status_by_premium = Window.partitionBy("holder_id", "status").orderBy("premium_bucket", "id")
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_all_by_days_since_prev_end_d = Window.orderBy("days_since_prev_end_date_2", "id").rowsBetween(-1, -1)
w_end_date_month_by_days_since = Window.partitionBy(F.month("end_date")).orderBy(F.col("days_since_prev_end_date").desc(), "id").rowsBetween(-1, -1)
w_all_by_days_since_prev_end_d_2 = Window.orderBy("days_since_prev_end_date_2", "id")
w_end_date_month_holder_id_by = Window.partitionBy(F.month("end_date"), "holder_id").orderBy(F.col("prev_premium").desc())

base = (
    df_policies
    .filter((F.col("status").isin("EXPIRED", "CANCELLED")) & (F.col("end_date") >= F.lit("2026-01-24").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

tagged = base.withColumn("end_date_year", F.year(F.col("end_date")))

with_windows = (
    tagged
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "end_date_year",
        F.lag("premium").over(w_all_by_premium).alias("prev_premium"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_by_premium)).alias("days_since_prev_end_date"),
        F.max("end_date_year").over(w_holder_id_by_premium_running).alias("max_end_date_year_so_far"),
        (F.col("premium_bucket") - F.lag("premium_bucket").over(w_holder_id_by_premium)).alias("premium_bucket_change"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_status_by_premium)).alias("days_since_prev_end_date_2"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_by_premium)).alias("days_since_prev_start_date"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "end_date_year",
        "prev_premium",
        "max_end_date_year_so_far",
        "premium_bucket_change",
        "days_since_prev_end_date_2",
        F.max("premium_bucket_change").over(w_product_holder_id).alias("product_holder_id_max_premium_bucket_change"),
        F.round(F.avg("end_date_year").over(w_all_by_days_since_prev_end_d), 2).alias("end_date_year_avg_1_prior"),
        F.round(F.avg("days_since_prev_end_date_2").over(w_end_date_month_by_days_since), 2).alias("days_since_prev_end_date_2_avg_1_prior"),
        (F.col("end_date_year") - F.lag("end_date_year").over(w_all_by_days_since_prev_end_d_2)).alias("end_date_year_change"),
        F.round(F.cume_dist().over(w_end_date_month_holder_id_by), 3).alias("prev_premium_cume_dist"),
    )
)

result = (
    windowed
    .rollup("holder_id", "end_date_year")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(~F.col("product").isin("AUTO", "HEALTH"), 1).otherwise(0)).alias("n_product_not_in_auto_health"),
        F.percentile_approx("days_since_prev_end_date_2", 0.5).alias("median_days_since_prev_end_date_2"),
    )
)
