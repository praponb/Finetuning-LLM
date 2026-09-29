from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_premium = Window.partitionBy("holder_id", "product").orderBy(F.col("premium_distance").desc(), "id").rowsBetween(-3, -1)
w_all_by_premium = Window.orderBy("premium", "id")
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy(F.col("premium_distance_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date = Window.orderBy("start_date", "id")
w_status = Window.partitionBy("status")
w_holder_id_product_by_premium_2 = Window.partitionBy("holder_id", "product").orderBy(F.col("premium_distance").desc(), "id")
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_all_by_premium_prior_6 = Window.orderBy("premium", "id").rowsBetween(-6, -1)
w_rn_start_date_rank = Window.partitionBy("rn", "start_date_rank")
w_all_by_premium_distance_2_ch = Window.orderBy(F.col("premium_distance_2_change_2").desc_nulls_first(), "id")
w_rn_by_rows_in_status_ties = Window.partitionBy("rn").orderBy(F.col("rows_in_status").asc_nulls_last())
w_all_by_premium_distance_2_ch_2 = Window.orderBy(F.col("premium_distance_2_change_2").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_distance_2_ch_3 = Window.orderBy(F.col("premium_distance_2_change_2").desc_nulls_first())

base = (
    df_policies
    .filter((F.col("holder_id") >= 11) & (F.col("status") == "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.abs(F.col("premium") - 2111.3).alias("premium_distance"),
        F.abs(F.col("premium") - 3465.4).alias("premium_distance_2"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.round(F.avg("premium_distance_2").over(w_holder_id_product_by_premium), 2).alias("premium_distance_2_avg_3_prior"),
        (F.col("premium_distance_2") - F.lag("premium_distance_2").over(w_all_by_premium)).alias("premium_distance_2_change"),
        F.sum("premium_distance_2").over(w_product_holder_id_by_premium).alias("running_premium_distance_2"),
        F.first("premium", ignorenulls=True).over(w_all_by_start_date_running).alias("first_known_premium"),
        F.rank().over(w_all_by_start_date).alias("start_date_rank"),
        F.round(F.avg("premium").over(w_status), 2).alias("status_avg_premium"),
        F.lag("premium").over(w_holder_id_product_by_premium_2).alias("prev_premium"),
        F.round(F.col("premium_distance_2") / F.nullif(F.sum("premium_distance_2").over(w_holder_id_product), F.lit(0)), 4).alias("premium_distance_2_share_of_holder_id_product"),
        F.round(F.avg("premium_distance").over(w_all_by_premium_prior_6), 2).alias("premium_distance_avg_6_prior"),
        F.row_number().over(w_holder_id_product_by_premium_2).alias("rn"),
        (F.col("premium_distance_2") - F.lag("premium_distance_2").over(w_holder_id_product_by_premium_2)).alias("premium_distance_2_change_2"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.size(F.collect_set("holder_id").over(w_rn_start_date_rank)).alias("distinct_holder_id_in_rn_start_date_rank"),
        F.ntile(5).over(w_all_by_premium_distance_2_ch).alias("premium_distance_2_change_2_quintile"),
        F.round(F.cume_dist().over(w_rn_by_rows_in_status_ties), 3).alias("rows_in_status_cume_dist"),
        F.last("premium_distance", ignorenulls=True).over(w_all_by_premium_distance_2_ch_2).alias("last_known_premium_distance"),
        F.size(F.collect_set("status").over(w_rn_start_date_rank)).alias("distinct_status_in_rn_start_date_rank"),
        F.dense_rank().over(w_all_by_premium_distance_2_ch_3).alias("premium_distance_2_change_2_dense_rank"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        "premium_distance",
        "premium_distance_2",
        "premium_distance_2_avg_3_prior",
        "premium_distance_2_change",
        "running_premium_distance_2",
        "start_date_rank",
        "premium_distance_2_share_of_holder_id_product",
        "premium_distance_avg_6_prior",
        "rn",
        "premium_distance_2_change_2",
        "rows_in_status",
        "distinct_holder_id_in_rn_start_date_rank",
        "premium_distance_2_change_2_quintile",
        "distinct_status_in_rn_start_date_rank",
        "premium_distance_2_change_2_dense_rank",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.when(F.col("first_known_premium") >= 2945.9, "large").when(F.col("first_known_premium") >= 1541.8, "medium").otherwise("small").alias("first_known_premium_tier"),
    )
    .orderBy(F.col("product").desc())
)
