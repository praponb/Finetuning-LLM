from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_product = Window.partitionBy(F.month("start_date"), "product")
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy(F.col("premium_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2025-04-17").cast("date"), F.lit("2027-01-03").cast("date"))) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

windowed = (
    base
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "premium_filled",
        F.round(F.col("premium_bucket") / F.nullif(F.sum("premium_bucket").over(w_start_date_month_product), F.lit(0)), 4).alias("premium_bucket_share_of_start_date_month_product"),
        F.last("start_date", ignorenulls=True).over(w_status_holder_id_by_premium).alias("last_known_start_date"),
        F.max("premium_filled").over(w_start_date_month_product).alias("start_date_month_product_max_premium_filled"),
    )
)

rollup_base = (
    windowed
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "holder_id",
        F.year("last_known_start_date").alias("last_known_start_date_year"),
    )
    .agg(
        F.percentile_approx("start_date_month_product_max_premium_filled", 0.5).alias("median_start_date_month_product_max_premium_fill"),
        F.round(F.sum("start_date_month_product_max_premium_filled") / F.nullif(F.sum("premium_bucket_share_of_start_date_month_product"), F.lit(0)), 3).alias("start_date_month_product_max_premium_filled_to_p"),
        F.count("*").alias("cnt"),
        F.round(F.sum("start_date_month_product_max_premium_filled"), 2).alias("total_start_date_month_product_max_premium_fille"),
        F.min("last_known_start_date").alias("first_last_known_start_date"),
        F.sum("premium_filled").alias("sum_premium_filled_check"),
    )
    .filter(F.col("sum_premium_filled_check") > 1640.0)
    .drop("sum_premium_filled_check")
)

with_activity = (
    rollup_base.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") > 2289.0) & (F.col("p.end_date").between(F.lit("2025-04-17").cast("date"), F.lit("2026-08-31").cast("date"))), "left_anti")
)

result = (
    with_activity
    .select(
        "start_date_month",
        "holder_id",
        "last_known_start_date_year",
        "median_start_date_month_product_max_premium_fill",
        "start_date_month_product_max_premium_filled_to_p",
        "cnt",
        "first_last_known_start_date",
        F.abs(F.col("cnt") - 8).alias("cnt_distance"),
    )
    .orderBy(
        F.col("start_date_month").desc_nulls_first(),
        "start_date_month_product_max_premium_filled_to_p",
    )
)
