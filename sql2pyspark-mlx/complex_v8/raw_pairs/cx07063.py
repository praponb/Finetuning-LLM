from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_start_date_to_en = Window.orderBy(F.col("days_start_date_to_end_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_holder_id_start_date_month_b = Window.partitionBy("holder_id", F.month("start_date")).orderBy("premium", "id")
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_holder_id_start_date_month_b_2 = Window.partitionBy("holder_id", F.month("start_date")).orderBy("premium")
w_start_date_month_product_by = Window.partitionBy(F.date_trunc("month", "start_date"), "product").orderBy("premium", "id")
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_holder_id_product_by_days_st = Window.partitionBy("holder_id", "product").orderBy("days_start_date_to_end_date", "id")
w_all_by_days_start_date_to_en_2 = Window.orderBy(F.col("days_start_date_to_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_product_by_days_st_2 = Window.partitionBy("holder_id", "product").orderBy("days_start_date_to_end_date")
w_all_by_start_date_full = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_policys = (
    df_policies
    .filter((F.col("status").isin("ACTIVE", "LAPSED", "CANCELLED")) & (F.col("product").isin("HEALTH", "AUTO", "HOME")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
    )
)

ranked = (
    valid_policys
    .select(
        "*",
        F.round(F.avg("days_start_date_to_end_date").over(w_all_by_days_start_date_to_en), 2).alias("days_start_date_to_end_date_avg_2"),
        (F.col("premium") - F.lag("premium").over(w_holder_id_start_date_month_b)).alias("premium_change"),
        F.row_number().over(w_all_by_start_date).alias("seq_no"),
        F.round(F.percent_rank().over(w_holder_id_start_date_month_b_2), 3).alias("premium_percent_rank"),
        F.dense_rank().over(w_start_date_month_product_by).alias("premium_dense_rank"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_product_by_days_st)).alias("days_since_prev_start_date"),
        F.round(F.col("days_start_date_to_end_date") - F.avg("days_start_date_to_end_date").over(w_holder_id_product), 2).alias("days_start_date_to_end_date_vs_holder_id_product"),
        F.first("start_date", ignorenulls=True).over(w_all_by_days_start_date_to_en_2).alias("first_known_start_date"),
        F.lead("start_date", 2).over(w_start_date_month_product_by).alias("next2_start_date"),
        F.dense_rank().over(w_holder_id_product_by_days_st_2).alias("days_start_date_to_end_date_dense_rank"),
        F.last("premium").over(w_all_by_start_date_full).alias("last_premium"),
        F.sum("days_start_date_to_end_date").over(w_holder_id_product).alias("holder_id_product_sum_days_start_date_to_end_dat"),
        F.min("premium").over(w_holder_id_product).alias("holder_id_product_min_premium"),
        F.lead("days_start_date_to_end_date").over(w_holder_id_start_date_month_b).alias("next_days_start_date_to_end_date"),
        F.rank().over(w_start_date_month_product_by).alias("premium_rank"),
    )
)

top_per_group = ranked.filter(F.col("days_start_date_to_end_date_dense_rank") <= 2)

result = (
    top_per_group
    .filter(F.col("days_start_date_to_end_date") <= 210)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "product_clean",
        "days_start_date_to_end_date_avg_2",
        "premium_change",
        "premium_dense_rank",
        "days_since_prev_start_date",
        "days_start_date_to_end_date_vs_holder_id_product",
        "first_known_start_date",
        "next2_start_date",
        "last_premium",
        "holder_id_product_sum_days_start_date_to_end_dat",
        "next_days_start_date_to_end_date",
        "premium_rank",
    )
    .orderBy(F.col("days_since_prev_start_date").desc_nulls_first())
)
