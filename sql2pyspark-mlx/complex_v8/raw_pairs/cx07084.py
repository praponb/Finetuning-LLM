from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_end_date_month_by = Window.partitionBy("holder_id", F.month("end_date")).orderBy("end_date", "id")
w_product_by_end_date_moving_3 = Window.partitionBy("product").orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)
w_product = Window.partitionBy("product")
w_product_by_premium = Window.partitionBy("product").orderBy(F.col("premium").desc(), "id")
w_all_by_premium = Window.orderBy(F.col("premium").desc_nulls_first(), "id")
w_product_by_premium_ties = Window.partitionBy("product").orderBy(F.col("premium").desc())

prepared = (
    df_policies
    .filter(F.col("premium") < 3817.5)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("month", F.col("end_date")).alias("end_date_month"),
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.ntile(4).over(w_holder_id_end_date_month_by).alias("end_date_quartile"),
        F.max("premium").over(w_product_by_end_date_moving_3).alias("premium_max_4"),
        F.sum("premium").over(w_product).alias("product_sum_premium"),
        F.dense_rank().over(w_product_by_premium).alias("premium_dense_rank"),
        F.round(F.col("premium") - F.avg("premium").over(w_product), 2).alias("premium_vs_product_avg"),
        F.lag("start_date").over(w_product_by_premium).alias("prev_start_date"),
        F.round(F.avg("premium").over(w_product), 2).alias("product_avg_premium"),
        F.row_number().over(w_all_by_premium).alias("rn"),
        F.rank().over(w_product_by_premium_ties).alias("premium_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.size(F.collect_set("status").over(w_product)).alias("distinct_status_in_product"),
    )
)

result = (
    ranked
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "end_date_month",
        "product_prefix",
        "premium_dense_rank",
        "prev_start_date",
        "product_avg_premium",
        "rn",
        "premium_rank",
        "distinct_status_in_product",
        F.concat_ws(": ", F.col("product"), F.col("product_prefix")).alias("product_product_prefix_label"),
    )
    .orderBy(F.col("product_avg_premium").desc_nulls_first(), "id")
    .limit(5)
)
