from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_group = Window.partitionBy("holder_id", "product_group")
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy(F.col("premium").asc_nulls_last(), "id")
w_product_status_by_premium_fu = Window.partitionBy("product", "status").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_holder_id_by_premium_2 = Window.partitionBy("product", "holder_id").orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status = Window.partitionBy("status")
w_all_by_premium = Window.orderBy("premium", "id")
w_product_status_by_premium = Window.partitionBy("product", "status").orderBy("premium", "id")
w_status_by_premium_bucket = Window.partitionBy("status").orderBy("premium_bucket", "id")
w_all_by_premium_full = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-03-15").cast("date"), F.lit("2025-05-30").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.when(F.col("product").isin("HOME", "TRAVEL"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.sum("premium_bucket").over(w_holder_id_product_group).alias("holder_id_product_group_sum_premium_bucket"),
        F.dense_rank().over(w_product_holder_id_by_premium).alias("premium_dense_rank"),
        F.nth_value("start_date", 2).over(w_product_status_by_premium_fu).alias("second_start_date"),
        F.last("start_date", ignorenulls=True).over(w_product_holder_id_by_premium_2).alias("last_known_start_date"),
        F.round(F.avg("premium_bucket").over(w_status), 2).alias("status_avg_premium_bucket"),
        F.round(F.col("premium_bucket") - F.avg("premium_bucket").over(w_status), 2).alias("premium_bucket_vs_status_avg"),
        (F.col("premium") - F.lag("premium").over(w_all_by_premium)).alias("premium_change"),
        F.row_number().over(w_product_status_by_premium).alias("premium_seq"),
        F.round(F.col("premium_bucket") / F.nullif(F.sum("premium_bucket").over(w_status), F.lit(0)), 4).alias("premium_bucket_share_of_status"),
        F.first("start_date", ignorenulls=True).over(w_product_holder_id_by_premium_2).alias("first_known_start_date"),
        F.ntile(4).over(w_status_by_premium_bucket).alias("premium_bucket_quartile"),
        F.dense_rank().over(w_all_by_premium).alias("premium_dense_rank_2"),
        F.first("premium_bucket").over(w_all_by_premium_full).alias("first_premium_bucket"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
    )
)

result = (
    ranked
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "premium_bucket",
        "product_group",
        "holder_id_product_group_sum_premium_bucket",
        "premium_dense_rank",
        "second_start_date",
        "last_known_start_date",
        "premium_bucket_vs_status_avg",
        "premium_change",
        "premium_bucket_quartile",
        "premium_dense_rank_2",
        "first_premium_bucket",
        F.round(F.col("premium_change") * 0.25, 2).alias("premium_change_adj"),
        F.round(F.col("premium_change") * 1.07, 2).alias("premium_change_adj_2"),
    )
    .orderBy("premium_change_adj_2", F.col("second_start_date").desc_nulls_first())
)
