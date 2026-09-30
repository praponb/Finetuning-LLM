from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_product_status_by_start_date = Window.partitionBy("product", "status").orderBy("start_date", "id")
w_product = Window.partitionBy("product")
w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc())
w_product_status_by_start_date_2 = Window.partitionBy("product", "status").orderBy("start_date")
w_start_date_rank_holder_id = Window.partitionBy("start_date_rank", "holder_id")
w_all_by_premium = Window.orderBy("premium", "id")
w_all_by_product_min_premium = Window.orderBy(F.col("product_min_premium").asc_nulls_last(), "id")

cleaned = (
    df_policies
    .filter(F.col("premium").between(1541.8, 3544.1))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.date_format(F.col("start_date"), "MMM yyyy").alias("start_date_label"),
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

sequenced = (
    cleaned
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        "product_prefix",
        F.round(F.avg("premium").over(w_status), 2).alias("status_avg_premium"),
        F.row_number().over(w_product_status_by_start_date).alias("seq_no"),
        F.round(F.col("premium") - F.avg("premium").over(w_product), 2).alias("premium_vs_product_avg"),
        F.min("premium").over(w_product).alias("product_min_premium"),
        F.round(F.cume_dist().over(w_all_by_start_date_ties), 3).alias("start_date_cume_dist"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.rank().over(w_product_status_by_start_date_2).alias("start_date_rank"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.size(F.collect_set("product").over(w_start_date_rank_holder_id)).alias("distinct_product_in_start_date_rank_holder_id"),
        F.lag("premium_share_of_product").over(w_all_by_premium).alias("prev_premium_share_of_product"),
        F.round(F.col("premium_vs_product_avg") / F.nullif(F.sum("premium_vs_product_avg").over(w_start_date_rank_holder_id), F.lit(0)), 4).alias("premium_vs_product_avg_share_of_start_date_rank_"),
        F.min("product_min_premium").over(w_start_date_rank_holder_id).alias("start_date_rank_holder_id_min_product_min_premiu"),
        F.lead("premium_vs_product_avg", 3).over(w_all_by_premium).alias("next3_premium_vs_product_avg"),
        (F.col("premium") - F.lag("premium").over(w_all_by_premium)).alias("premium_change"),
        F.ntile(4).over(w_all_by_product_min_premium).alias("product_min_premium_quartile"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        "product_prefix",
        "status_avg_premium",
        "seq_no",
        "premium_vs_product_avg",
        "product_min_premium",
        "start_date_cume_dist",
        "premium_share_of_product",
        "start_date_rank",
        "prev_premium_share_of_product",
        "premium_vs_product_avg_share_of_start_date_rank_",
        "start_date_rank_holder_id_min_product_min_premiu",
    )
    .orderBy("premium_share_of_product", F.col("id").desc())
    .limit(20)
)
