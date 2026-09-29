from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id")
w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_month_star = Window.orderBy("start_date_month_start", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_month_star_2 = Window.orderBy("start_date_month_start", "id").rowsBetween(-6, -1)
w_all_by_start_date_month_star_3 = Window.orderBy("start_date_month_start")
w_all_by_start_date_month_star_4 = Window.orderBy("start_date_month_start", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_month_start_month = Window.partitionBy(F.month("start_date_month_start"))
w_product_by_start_date_ties = Window.partitionBy("product").orderBy(F.col("start_date").desc())
w_holder_id_by_premium_moving = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(-2, Window.currentRow)

scoped = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-07-07").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.row_number().over(w_holder_id_by_premium).alias("seq_no"),
        F.first("start_date_month_start", ignorenulls=True).over(w_holder_id_by_premium_running).alias("first_known_start_date_month_start"),
        F.last("premium_filled", ignorenulls=True).over(w_all_by_start_date_month_star).alias("last_known_premium_filled"),
        F.round(F.avg("premium").over(w_all_by_start_date_month_star_2), 2).alias("premium_avg_6_prior"),
        F.rank().over(w_all_by_start_date_month_star_3).alias("start_date_month_start_rank"),
        F.first("premium").over(w_all_by_start_date_month_star_4).alias("first_premium"),
        F.size(F.collect_set("status").over(w_start_date_month_start_month)).alias("distinct_status_in_start_date_month_start_month"),
        F.dense_rank().over(w_product_by_start_date_ties).alias("start_date_dense_rank"),
        F.dense_rank().over(w_all_by_start_date_month_star_3).alias("start_date_month_start_dense_rank"),
        F.count(F.lit(1)).over(w_start_date_month_start_month).alias("rows_in_start_date_month_start_month"),
        F.round(F.avg("premium").over(w_holder_id_by_premium_moving), 2).alias("premium_avg_3"),
        F.min("premium_filled").over(w_start_date_month_start_month).alias("start_date_month_start_month_min_premium_filled"),
        F.size(F.collect_set("product").over(w_start_date_month_start_month)).alias("distinct_product_in_start_date_month_start_month"),
        F.nth_value("premium_filled", 2).over(w_all_by_start_date_month_star_4).alias("second_premium_filled"),
    )
)

annotated = (
    ranked
    .filter(F.col("start_date_month_start_month_min_premium_filled") > 17630.0)
    .withColumn("premium_filled_filled", F.coalesce(F.col("premium_filled"), F.lit(0.0)))
)

per_group = (
    annotated
    .filter(F.col("start_date_month_start_month_min_premium_filled").between(7670.0, 32570.0))
    .groupBy(
        "seq_no",
        F.date_trunc("month", "start_date_month_start").alias("start_date_month_start_month"),
        F.date_trunc("month", "start_date").alias("start_date_month"),
    )
    .agg(
        F.round(F.sum("start_date_dense_rank") / F.nullif(F.sum("distinct_product_in_start_date_month_start_month"), F.lit(0)), 3).alias("start_date_dense_rank_to_distinct_product_in_sta"),
        F.percentile_approx("start_date_month_start_rank", 0.5).alias("median_start_date_month_start_rank"),
        F.min("second_premium_filled").alias("min_second_premium_filled"),
        F.max("start_date").alias("last_start_date"),
        F.count("*").alias("n_rows"),
        F.countDistinct("product").alias("distinct_products"),
    )
)

result = (
    per_group
    .select(
        "seq_no",
        "start_date_month_start_month",
        "start_date_dense_rank_to_distinct_product_in_sta",
        "median_start_date_month_start_rank",
        "min_second_premium_filled",
        "last_start_date",
        "n_rows",
        "distinct_products",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_start_date")).alias("days_since_last_start_date"),
        F.when(F.col("distinct_products") >= 8, "A").when(F.col("distinct_products") >= 5, "B").otherwise("C").alias("distinct_products_tier"),
    )
    .orderBy(F.col("seq_no").desc(), F.col("last_start_date").desc())
)
