from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_premium = Window.partitionBy("product").orderBy(F.col("premium").desc(), "id")
w_holder_id_product_group = Window.partitionBy("holder_id", "product_group")
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy("premium", "id")
w_product_group_product = Window.partitionBy("product_group", "product")
w_product_group_by_start_date = Window.partitionBy("product_group").orderBy("start_date", "id")
w_product_group_by_start_date_2 = Window.partitionBy("product_group").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w_product_group_by_start_date_3 = Window.partitionBy("product_group").orderBy("start_date")
w_distinct_status_in_holder_id = Window.partitionBy("distinct_status_in_holder_id_product_group")
w_all_by_premium_share_of_hold = Window.orderBy("premium_share_of_holder_id_product_group")
w_rn_distinct_holder_id_in_pro = Window.partitionBy("rn", "distinct_holder_id_in_product_group_product").orderBy(F.col("holder_id_product_group_min_premium").desc(), "id")

recent = (
    df_policies
    .filter((F.col("end_date") < F.lit("2026-08-31").cast("date")) | (F.col("product") == "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.when(F.col("product").isin("AUTO", "TRAVEL"), "group_a").when(F.col("product") == "HEALTH", "group_b").otherwise("other").alias("product_group"),
    )
)

with_trends = (
    recent
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_product_by_premium)).alias("days_since_prev_start_date"),
        F.min("premium").over(w_holder_id_product_group).alias("holder_id_product_group_min_premium"),
        F.size(F.collect_set("status").over(w_holder_id_product_group)).alias("distinct_status_in_holder_id_product_group"),
        F.max("premium").over(w_holder_id_product_group).alias("holder_id_product_group_max_premium"),
        F.row_number().over(w_holder_id_by_premium).alias("rn"),
        F.size(F.collect_set("holder_id").over(w_product_group_product)).alias("distinct_holder_id_in_product_group_product"),
        F.round(F.percent_rank().over(w_product_group_by_start_date), 3).alias("start_date_percent_rank"),
        F.round(F.avg("premium").over(w_product_group_product), 2).alias("product_group_product_avg_premium"),
        F.count(F.lit(1)).over(w_product_group_product).alias("rows_in_product_group_product"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_group_product), F.lit(0)), 4).alias("premium_share_of_product_group_product"),
        F.round(F.avg("premium").over(w_product_group_by_start_date_2), 2).alias("premium_avg_7"),
        F.rank().over(w_product_group_by_start_date_3).alias("start_date_rank"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_product_group), 2).alias("premium_vs_holder_id_product_group_avg"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_product_group), F.lit(0)), 4).alias("premium_share_of_holder_id_product_group"),
    )
)

first_rows = with_trends.filter(F.col("start_date_rank") <= 2)

with_trends_2 = (
    first_rows
    .select(
        "*",
        F.count(F.lit(1)).over(w_distinct_status_in_holder_id).alias("rows_in_distinct_status_in_holder_id_product_gro"),
        F.dense_rank().over(w_all_by_premium_share_of_hold).alias("premium_share_of_holder_id_product_group_dense_r"),
        F.lag("premium_avg_7").over(w_rn_distinct_holder_id_in_pro).alias("prev_premium_avg_7"),
    )
)

result = (
    with_trends_2
    .filter(F.col("distinct_holder_id_in_product_group_product") < 9)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "days_since_prev_start_date",
        "holder_id_product_group_min_premium",
        "distinct_status_in_holder_id_product_group",
        "holder_id_product_group_max_premium",
        "rn",
        "distinct_holder_id_in_product_group_product",
        "start_date_percent_rank",
        "product_group_product_avg_premium",
        "rows_in_product_group_product",
        "premium_share_of_product_group_product",
        "start_date_rank",
        "premium_share_of_holder_id_product_group",
        "rows_in_distinct_status_in_holder_id_product_gro",
        "premium_share_of_holder_id_product_group_dense_r",
        "prev_premium_avg_7",
    )
)
