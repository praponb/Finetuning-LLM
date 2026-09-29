from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium_distance = Window.orderBy("premium_distance", "id")
w_holder_id_start_date_year = Window.partitionBy("holder_id", F.year("start_date"))
w_all_by_premium_distance_prio = Window.orderBy("premium_distance", "id").rowsBetween(-6, -1)
w_start_date_month_end_month_p = Window.partitionBy(F.month("start_date_month_end"), "premium_distance_decile")
w_all_by_distinct_product_in_h = Window.orderBy("distinct_product_in_holder_id_start_date_year", "id")
w_all_by_distinct_product_in_h_2 = Window.orderBy("distinct_product_in_holder_id_start_date_year", "id").rowsBetween(-1, Window.currentRow)
w_holder_id_by_distinct_produc = Window.partitionBy("holder_id").orderBy("distinct_product_in_holder_id_start_date_year", "id").rowsBetween(-6, Window.currentRow)
w_holder_id_by_distinct_produc_2 = Window.partitionBy("holder_id").orderBy("distinct_product_in_holder_id_start_date_year", "id")
w_distinct_holder_id_in_start = Window.partitionBy("distinct_holder_id_in_start_date_month_end_month", "status").orderBy(F.col("premium_distance_decile_max_7").desc(), "id")

recent_policys = (
    df_policies
    .filter(F.col("product").isin("AUTO", "HEALTH", "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
        F.last_day(F.col("start_date")).alias("start_date_month_end"),
        F.abs(F.col("premium") - 2096.6).alias("premium_distance"),
    )
)

without_matches = (
    recent_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") != "OPEN") & (F.col("c.claim_date").isNotNull()), "left_anti")
    .filter(F.col("status_prefix").isNotNull())
)

with_trends = (
    without_matches
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        "status_prefix",
        "start_date_month_end",
        "premium_distance",
        F.row_number().over(w_all_by_premium_distance).alias("premium_distance_seq"),
        F.size(F.collect_set("product").over(w_holder_id_start_date_year)).alias("distinct_product_in_holder_id_start_date_year"),
        F.ntile(10).over(w_all_by_premium_distance).alias("premium_distance_decile"),
        F.round(F.avg("premium_distance").over(w_all_by_premium_distance_prio), 2).alias("premium_distance_avg_6_prior"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "status_prefix",
        "start_date_month_end",
        "premium_distance",
        "premium_distance_decile",
        "premium_distance_avg_6_prior",
        F.size(F.collect_set("holder_id").over(w_start_date_month_end_month_p)).alias("distinct_holder_id_in_start_date_month_end_month"),
        F.ntile(4).over(w_all_by_distinct_product_in_h).alias("distinct_product_in_holder_id_start_date_year_qu"),
        F.sum("premium_distance").over(w_all_by_distinct_product_in_h_2).alias("premium_distance_sum_2"),
        F.max("premium_distance_decile").over(w_holder_id_by_distinct_produc).alias("premium_distance_decile_max_7"),
        F.rank().over(w_holder_id_by_distinct_produc_2).alias("distinct_product_in_holder_id_start_date_year_ra"),
    )
)

with_trends_3 = (
    with_trends_2
    .select(
        "*",
        F.dense_rank().over(w_distinct_holder_id_in_start).alias("premium_distance_decile_max_7_dense_rank"),
        F.row_number().over(w_distinct_holder_id_in_start).alias("seq_no"),
    )
)

result = (
    with_trends_3
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "status_prefix",
        "start_date_month_end",
        "premium_distance_decile",
        "premium_distance_avg_6_prior",
        "distinct_holder_id_in_start_date_month_end_month",
        "distinct_product_in_holder_id_start_date_year_qu",
        "premium_distance_sum_2",
        "premium_distance_decile_max_7",
        "premium_distance_decile_max_7_dense_rank",
    )
    .orderBy(F.col("status_prefix").desc(), F.col("premium_distance_decile").asc())
)
