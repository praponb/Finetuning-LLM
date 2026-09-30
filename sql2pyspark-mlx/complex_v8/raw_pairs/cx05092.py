from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_filled_cnt_by_cnt_ties = Window.partitionBy("cnt_filled", "cnt").orderBy(F.col("cnt").asc_nulls_last())
w_cnt_filled_cnt_by_cnt_runnin = Window.partitionBy("cnt_filled", "cnt").orderBy(F.col("cnt").asc_nulls_last(), "end_date_ym", "product_list_item").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_cnt = Window.partitionBy("cnt")
w_all_by_cnt_max_cnt_ties = Window.orderBy(F.col("cnt_max_cnt").desc())
w_cnt_bucket = Window.partitionBy("cnt_bucket")

filtered_policys = (
    df_policies
    .filter(F.col("status") != "EXPIRED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
    )
)

per_group = (
    filtered_policys
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_date_ym"))
    .agg(
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

scored = (
    per_group
    .select(
        "*",
        F.length(F.trim(F.col("end_date_ym"))).alias("end_date_ym_len"),
        F.lower(F.trim(F.col("end_date_ym"))).alias("end_date_ym_clean"),
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
    )
)

exploded = (
    scored
    .select(
        "end_date_ym",
        "cnt",
        "end_date_ym_len",
        "end_date_ym_clean",
        "cnt_bucket",
        F.explode("product_list").alias("product_list_item"),
    )
)

tagged = (
    exploded
    .select(
        "end_date_ym",
        "cnt",
        "end_date_ym_len",
        "cnt_bucket",
        "product_list_item",
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
        F.substring(F.trim(F.col("product_list_item")), 1, 3).alias("product_list_item_prefix"),
    )
)

with_trends = (
    tagged
    .select(
        "*",
        F.round(F.cume_dist().over(w_cnt_filled_cnt_by_cnt_ties), 3).alias("cnt_cume_dist"),
        F.last("cnt_bucket", ignorenulls=True).over(w_cnt_filled_cnt_by_cnt_runnin).alias("last_known_cnt_bucket"),
        F.round(F.percent_rank().over(w_cnt_filled_cnt_by_cnt_ties), 3).alias("cnt_percent_rank"),
        F.max("cnt").over(w_cnt).alias("cnt_max_cnt"),
    )
)

windowed = (
    with_trends
    .select(
        "*",
        F.rank().over(w_all_by_cnt_max_cnt_ties).alias("cnt_max_cnt_rank"),
        F.round(F.col("cnt_cume_dist") - F.avg("cnt_cume_dist").over(w_cnt_bucket), 2).alias("cnt_cume_dist_vs_cnt_bucket_avg"),
        F.round(F.cume_dist().over(w_all_by_cnt_max_cnt_ties), 3).alias("cnt_max_cnt_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "end_date_ym",
        "cnt",
        "end_date_ym_len",
        "cnt_bucket",
        "product_list_item_prefix",
        "cnt_cume_dist",
        "last_known_cnt_bucket",
        "cnt_percent_rank",
        "cnt_max_cnt",
        "cnt_cume_dist_vs_cnt_bucket_avg",
        "cnt_max_cnt_cume_dist",
        F.concat(F.col("product_list_item_prefix"), F.lit("#"), F.col("cnt_bucket").cast("string")).alias("product_list_item_prefix_ref"),
    )
    .orderBy("cnt_bucket")
)
