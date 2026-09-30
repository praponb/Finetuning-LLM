from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_status_holder_id_by_status_l = Window.partitionBy("status", "holder_id").orderBy("status_len", "id")
w_end_date_month_holder_id_by = Window.partitionBy(F.date_trunc("month", "end_date"), "holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_holder_id_by_status_l_2 = Window.partitionBy("status", "holder_id").orderBy("status_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_len = Window.partitionBy("status_len")
w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_end_date_month_holder_id_by_2 = Window.partitionBy(F.date_trunc("month", "end_date"), "holder_id").orderBy(F.col("start_date").desc())
w_end_date_month_holder_id_by_3 = Window.partitionBy(F.date_trunc("month", "end_date"), "holder_id").orderBy(F.col("start_date").desc(), "id")
w_all_by_premium_full = Window.orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_ties = Window.orderBy(F.col("premium").desc())
w_all_by_premium_moving_2 = Window.orderBy(F.col("premium").desc(), "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_policies
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.min("status_len").over(w_product).alias("product_min_status_len"),
        (F.col("premium_adj") - F.lag("premium_adj").over(w_status_holder_id_by_status_l)).alias("premium_adj_change"),
        F.first("premium", ignorenulls=True).over(w_end_date_month_holder_id_by).alias("first_known_premium"),
        F.first("premium_adj", ignorenulls=True).over(w_status_holder_id_by_status_l_2).alias("first_known_premium_adj"),
        F.round(F.avg("status_len").over(w_status_len), 2).alias("status_len_avg_status_len"),
        F.size(F.collect_set("holder_id").over(w_product)).alias("distinct_holder_id_in_product"),
        F.lead("start_date").over(w_all_by_premium).alias("next_start_date"),
        F.round(F.percent_rank().over(w_end_date_month_holder_id_by_2), 3).alias("start_date_percent_rank"),
        F.ntile(3).over(w_end_date_month_holder_id_by_3).alias("start_date_tercile"),
        F.row_number().over(w_all_by_premium).alias("premium_seq"),
        F.nth_value("status_len", 2).over(w_all_by_premium_full).alias("second_status_len"),
        F.round(F.cume_dist().over(w_all_by_premium_ties), 3).alias("premium_cume_dist"),
        F.max("status_len").over(w_all_by_premium_moving_2).alias("status_len_max_3"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
    )
)

derived = (
    sequenced
    .filter(F.col("premium_seq") >= 4)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status_len",
        "premium_adj",
        "product_min_status_len",
        "premium_adj_change",
        "first_known_premium_adj",
        "status_len_avg_status_len",
        "distinct_holder_id_in_product",
        "start_date_percent_rank",
        "start_date_tercile",
        "second_status_len",
        "premium_cume_dist",
        "status_len_max_3",
        "rows_in_product",
        F.when(F.col("status").isin("CANCELLED", "LAPSED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
        (F.col("premium_seq") - F.col("premium")).alias("premium_seq_minus_premium"),
    )
)

result = (
    derived
    .filter(F.col("start_date") < F.lit("2025-02-14").cast("date"))
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status_len",
        "product_min_status_len",
        "premium_adj_change",
        "first_known_premium_adj",
        "status_len_avg_status_len",
        "distinct_holder_id_in_product",
        "start_date_percent_rank",
        "start_date_tercile",
        "second_status_len",
        "status_len_max_3",
        "rows_in_product",
        "status_group",
        F.round(F.col("premium_adj") * 0.25, 2).alias("premium_adj_adj"),
        F.round(F.col("start_date_percent_rank") / F.nullif(F.col("premium_cume_dist"), F.lit(0)), 3).alias("start_date_percent_rank_per_premium_cume_dist"),
    )
    .orderBy(F.col("status_len").asc(), F.col("start_date_tercile").asc())
)
