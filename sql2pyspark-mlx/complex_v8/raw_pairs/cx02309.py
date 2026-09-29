from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_end_date = Window.partitionBy("product").orderBy(F.col("end_date").desc(), "id")
w_product_status_by_start_date = Window.partitionBy("product", "status").orderBy(F.col("start_date").desc())
w_product_by_end_date_ties = Window.partitionBy("product").orderBy(F.col("end_date").desc())
w_status_by_start_date = Window.partitionBy("status").orderBy("start_date", "id")
w_product_status_by_start_date_2 = Window.partitionBy("product", "status").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_start_date_running = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_status_by_start_date_3 = Window.partitionBy("product", "status").orderBy(F.col("start_date").desc(), "id")
w_all_by_start_date_percent_ra = Window.orderBy(F.col("start_date_percent_rank").desc(), "id")
w_start_date_rank_end_date_seq = Window.partitionBy("start_date_rank", "end_date_seq")

cleaned_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(" / ", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

with_trends = (
    cleaned_policys
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "product_status_label",
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_product_by_end_date)).alias("days_since_prev_end_date"),
        F.round(F.cume_dist().over(w_product_status_by_start_date), 3).alias("start_date_cume_dist"),
        F.dense_rank().over(w_product_by_end_date_ties).alias("end_date_dense_rank"),
        F.ntile(4).over(w_status_by_start_date).alias("start_date_quartile"),
        F.last("start_date").over(w_product_status_by_start_date_2).alias("last_start_date"),
        F.first("start_date", ignorenulls=True).over(w_status_by_start_date_running).alias("first_known_start_date"),
        F.ntile(3).over(w_product_status_by_start_date_3).alias("start_date_tercile"),
        F.round(F.percent_rank().over(w_product_status_by_start_date_3), 3).alias("start_date_percent_rank"),
        F.row_number().over(w_product_by_end_date).alias("end_date_seq"),
        F.lead("end_date").over(w_status_by_start_date).alias("next_end_date"),
        F.rank().over(w_product_status_by_start_date).alias("start_date_rank"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.row_number().over(w_all_by_start_date_percent_ra).alias("rn"),
        F.size(F.collect_set("product").over(w_start_date_rank_end_date_seq)).alias("distinct_product_in_start_date_rank_end_date_seq"),
        F.size(F.collect_set("holder_id").over(w_start_date_rank_end_date_seq)).alias("distinct_holder_id_in_start_date_rank_end_date_s"),
    )
)

result = (
    sequenced
    .filter(F.col("rn") <= 9)
    .select(
        "id",
        "product",
        "start_date",
        "product_status_label",
        "days_since_prev_end_date",
        "start_date_cume_dist",
        "end_date_dense_rank",
        "start_date_quartile",
        "first_known_start_date",
        "start_date_tercile",
        "start_date_percent_rank",
        "next_end_date",
        "start_date_rank",
        "rn",
        "distinct_product_in_start_date_rank_end_date_seq",
        "distinct_holder_id_in_start_date_rank_end_date_s",
        F.when(F.col("product").isin("HEALTH", "TRAVEL"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
    )
    .orderBy(
        F.col("distinct_product_in_start_date_rank_end_date_seq").desc(),
        F.col("days_since_prev_end_date").desc(),
    )
)
