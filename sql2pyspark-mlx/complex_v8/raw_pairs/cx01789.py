from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date_full = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_holder_id_by_start_d = Window.partitionBy("product", "holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_holder_id_by_start_d_2 = Window.partitionBy("product", "holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc())
w_product_holder_id_by_start_d_3 = Window.partitionBy("product", "holder_id").orderBy("start_date")
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_all_by_first_start_date_runn = Window.orderBy(F.col("first_start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_start_date_year_h = Window.partitionBy(F.year("last_known_start_date"), "holder_id")
w_product_by_days_since_prev_s = Window.partitionBy("product").orderBy("days_since_prev_start_date", "id").rowsBetween(-2, Window.currentRow)
w_product_start_date_month_by = Window.partitionBy("product", F.month("start_date")).orderBy(F.col("start_date_dense_rank").desc(), "id")
w_product_by_days_since_prev_s_2 = Window.partitionBy("product").orderBy("days_since_prev_start_date", "id").rowsBetween(-1, -1)
w_all_by_first_start_date_movi = Window.orderBy(F.col("first_start_date").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_first_start_date = Window.orderBy(F.col("first_start_date").desc_nulls_first(), "id")

scoped = (
    df_policies
    .filter((F.col("product") == "AUTO") & (F.year(F.col("start_date")) == 2025))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

with_trends = (
    scoped
    .select(
        "*",
        F.nth_value("start_date", 2).over(w_all_by_end_date_full).alias("second_start_date"),
        F.last("start_date", ignorenulls=True).over(w_product_holder_id_by_start_d).alias("last_known_start_date"),
        F.last("start_date").over(w_product_holder_id_by_start_d_2).alias("last_start_date"),
        F.first("start_date").over(w_product_holder_id_by_start_d_2).alias("first_start_date"),
        F.dense_rank().over(w_all_by_start_date_ties).alias("start_date_dense_rank"),
        F.round(F.cume_dist().over(w_product_holder_id_by_start_d_3), 3).alias("start_date_cume_dist"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_all_by_end_date)).alias("days_since_prev_start_date"),
        F.round(F.percent_rank().over(w_all_by_start_date), 3).alias("start_date_percent_rank"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "first_start_date",
        "start_date_cume_dist",
        "start_date_percent_rank",
        F.min("start_date_percent_rank").over(w_all_by_first_start_date_runn).alias("min_start_date_percent_rank_so_far"),
        F.round(F.col("start_date_dense_rank") - F.avg("start_date_dense_rank").over(w_last_known_start_date_year_h), 2).alias("start_date_dense_rank_vs_last_known_start_date_y"),
        F.max("start_date_dense_rank").over(w_product_by_days_since_prev_s).alias("start_date_dense_rank_max_3"),
        F.round(F.cume_dist().over(w_product_start_date_month_by), 3).alias("start_date_dense_rank_cume_dist"),
        F.max("start_date_percent_rank").over(w_product_by_days_since_prev_s_2).alias("start_date_percent_rank_max_1_prior"),
        F.round(F.avg("start_date_percent_rank").over(w_last_known_start_date_year_h), 2).alias("last_known_start_date_year_holder_id_avg_start_d"),
        F.max("start_date_percent_rank").over(w_all_by_first_start_date_movi).alias("start_date_percent_rank_max_7"),
        F.ntile(10).over(w_all_by_first_start_date).alias("first_start_date_decile"),
    )
)

result = (
    sequenced
    .filter(F.col("min_start_date_percent_rank_so_far") >= 3.0)
    .select(
        "id",
        "end_date",
        "status",
        "start_date_cume_dist",
        "start_date_percent_rank",
        "min_start_date_percent_rank_so_far",
        "start_date_dense_rank_max_3",
        "start_date_dense_rank_cume_dist",
        "start_date_percent_rank_max_1_prior",
        "last_known_start_date_year_holder_id_avg_start_d",
        "first_start_date_decile",
        F.round(F.col("start_date_percent_rank") / F.nullif(F.col("first_start_date_decile"), F.lit(0)), 3).alias("start_date_percent_rank_per_first_start_date_dec"),
    )
    .orderBy("id")
)
