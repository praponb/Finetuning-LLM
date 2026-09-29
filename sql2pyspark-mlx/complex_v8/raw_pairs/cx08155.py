from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_id_by_start_d = Window.partitionBy("product", "holder_id").orderBy(F.col("start_date").desc(), "id")
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_year_product_by_s = Window.partitionBy(F.year("start_date"), "product").orderBy("start_date", "id")
w_holder_id_start_date_month_b = Window.partitionBy("holder_id", F.month("start_date")).orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_holder_id_by_start_d_2 = Window.partitionBy("product", "holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_holder_id_by_start_d_3 = Window.partitionBy("product", "holder_id").orderBy(F.col("start_date").desc())
w_holder_id_start_date_month_b_2 = Window.partitionBy("holder_id", F.month("start_date")).orderBy("start_date", "id")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2025-11-20").cast("date")) & (F.col("start_date") >= F.lit("2024-11-20").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.rank().over(w_product_holder_id_by_start_d).alias("start_date_rank"),
        F.last("start_date", ignorenulls=True).over(w_all_by_end_date_running).alias("last_known_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_product_holder_id_by_start_d)).alias("days_since_prev_start_date"),
        F.lead("start_date").over(w_start_date_year_product_by_s).alias("next_start_date"),
        F.nth_value("start_date", 2).over(w_holder_id_start_date_month_b).alias("second_start_date"),
        F.first("start_date", ignorenulls=True).over(w_product_holder_id_by_start_d_2).alias("first_known_start_date"),
        F.rank().over(w_product_holder_id_by_start_d_3).alias("start_date_rank_2"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_start_date_month_b_2)).alias("days_since_prev_end_date"),
        F.lag("start_date").over(w_all_by_end_date).alias("prev_start_date"),
        F.first("start_date").over(w_all_by_end_date_full).alias("first_start_date"),
        F.round(F.cume_dist().over(w_product_holder_id_by_start_d_3), 3).alias("start_date_cume_dist"),
        F.dense_rank().over(w_holder_id_start_date_month_b_2).alias("start_date_dense_rank"),
        F.lag("start_date").over(w_holder_id_start_date_month_b_2).alias("prev_start_date_2"),
        F.rank().over(w_start_date_year_product_by_s).alias("start_date_rank_3"),
        F.last("start_date").over(w_all_by_end_date_full).alias("last_start_date"),
    )
)

enriched = (
    with_trends
    .withColumn("start_date_rank_filled", F.coalesce(F.col("start_date_rank"), F.lit(0)))
)

result = (
    enriched
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "last_known_start_date",
        "days_since_prev_start_date",
        "next_start_date",
        "second_start_date",
        "first_known_start_date",
        "start_date_rank_2",
        "days_since_prev_end_date",
        "start_date_cume_dist",
        "start_date_dense_rank",
        "prev_start_date_2",
        "start_date_rank_3",
        "last_start_date",
        "start_date_rank_filled",
    )
    .orderBy(F.col("start_date_dense_rank").desc(), F.col("start_date_rank_3").asc())
)
