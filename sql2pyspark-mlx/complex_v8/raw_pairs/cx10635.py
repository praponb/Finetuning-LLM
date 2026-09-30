from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id")
w_all_by_premium_moving_3 = Window.orderBy("premium", "id").rowsBetween(-3, Window.currentRow)
w_product_by_end_date_moving_1 = Window.partitionBy("product").orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_full = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_premium_moving = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(-1, Window.currentRow)
w_product_by_end_date_full = Window.partitionBy("product").orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("end_date") >= F.lit("2026-01-23").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
    )
)

sequenced = (
    recent_policys
    .select(
        "*",
        F.round(F.percent_rank().over(w_holder_id_by_premium), 3).alias("premium_percent_rank"),
        F.round(F.cume_dist().over(w_holder_id_by_premium), 3).alias("premium_cume_dist"),
        F.round(F.avg("premium").over(w_all_by_premium_moving_3), 2).alias("premium_avg_4"),
        F.max("premium").over(w_product_by_end_date_moving_1).alias("premium_max_2"),
        F.last("start_date", ignorenulls=True).over(w_holder_id_by_premium_running).alias("last_known_start_date"),
        F.nth_value("start_date", 2).over(w_all_by_premium_full).alias("second_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_by_premium)).alias("days_since_prev_start_date"),
        F.max("premium").over(w_holder_id_by_premium_moving).alias("premium_max_2_2"),
        F.nth_value("start_date", 2).over(w_product_by_end_date_full).alias("second_start_date_2"),
    )
)

labeled = (
    sequenced
    .select(
        "*",
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
        F.date_format(F.col("last_known_start_date"), "MMM yyyy").alias("last_known_start_date_label"),
    )
)

result = (
    labeled
    .select(
        "id",
        "product",
        "start_date",
        "end_date",
        "status",
        "premium_percent_rank",
        "premium_cume_dist",
        "last_known_start_date",
        "days_since_prev_start_date",
        "premium_max_2_2",
        "second_start_date_2",
        "last_known_start_date_label",
        F.add_months(F.col("start_date"), 3).alias("start_date_plus_3m"),
        F.round(F.col("premium_avg_4") * 1.1, 2).alias("premium_avg_4_adj"),
    )
)
