from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_product = Window.partitionBy("holder_id", "product").orderBy("product_len", "id")
w_start_date_month_by_premium = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("premium").desc(), "id")
w_holder_id_status = Window.partitionBy("holder_id", "status")
w_start_date_month_by_start_da = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy("start_date", "id")
w_holder_id_product_by_product_2 = Window.partitionBy("holder_id", "product").orderBy("product_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_product_by_product_3 = Window.partitionBy("holder_id", "product").orderBy("product_len")
w_start_date_month_by_start_da_2 = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy("start_date")

prepared = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-09-24").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

metrics = (
    prepared
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "days_start_date_to_end_date",
        "product_len",
        F.lead("premium", 2).over(w_holder_id_product_by_product).alias("next2_premium"),
        F.round(F.cume_dist().over(w_start_date_month_by_premium), 3).alias("premium_cume_dist"),
        F.min("premium").over(w_holder_id_status).alias("holder_id_status_min_premium"),
        F.row_number().over(w_start_date_month_by_start_da).alias("start_date_seq"),
        F.ntile(4).over(w_holder_id_product_by_product).alias("product_len_quartile"),
        F.sum("days_start_date_to_end_date").over(w_holder_id_status).alias("holder_id_status_sum_days_start_date_to_end_date"),
        F.lag("premium").over(w_holder_id_product_by_product).alias("prev_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_status), 2).alias("premium_vs_holder_id_status_avg"),
        F.rank().over(w_start_date_month_by_premium).alias("premium_rank"),
        F.sum("product_len").over(w_holder_id_status).alias("holder_id_status_sum_product_len"),
        F.round(F.avg("days_start_date_to_end_date").over(w_holder_id_status), 2).alias("holder_id_status_avg_days_start_date_to_end_date"),
        F.size(F.collect_set("product").over(w_holder_id_status)).alias("distinct_product_in_holder_id_status"),
        F.first("end_date", ignorenulls=True).over(w_holder_id_product_by_product_2).alias("first_known_end_date"),
        F.dense_rank().over(w_holder_id_product_by_product_3).alias("product_len_dense_rank"),
        F.rank().over(w_start_date_month_by_start_da_2).alias("start_date_rank"),
        F.lead("premium").over(w_start_date_month_by_premium).alias("next_premium"),
    )
)

result = (
    metrics
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "days_start_date_to_end_date",
        "product_len",
        "next2_premium",
        "premium_cume_dist",
        "holder_id_status_min_premium",
        "start_date_seq",
        "prev_premium",
        "premium_rank",
        "holder_id_status_sum_product_len",
        "holder_id_status_avg_days_start_date_to_end_date",
        "first_known_end_date",
        "product_len_dense_rank",
        "start_date_rank",
        F.when(F.col("product").isin("AUTO", "LIFE"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
        F.length(F.trim(F.col("product"))).alias("product_len_2"),
    )
    .orderBy(F.col("start_date_seq").asc(), "id")
    .limit(5)
)
