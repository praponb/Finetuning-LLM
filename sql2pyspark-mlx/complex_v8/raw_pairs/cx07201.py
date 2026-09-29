from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_end_date_month = Window.partitionBy("holder_id", F.month("end_date"))
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_holder_id_by_premium_3 = Window.partitionBy("status", "holder_id").orderBy(F.col("premium").asc_nulls_last())
w_all_by_end_date_plus_7d = Window.orderBy(F.col("end_date_plus_7d").desc(), "id")
w_end_date_plus_7d_month_produ = Window.partitionBy(F.month("end_date_plus_7d"), "product").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_month_month = Window.partitionBy(F.date_trunc("month", "end_date_month"))
w_n_product_travel_end_date_mo = Window.partitionBy("n_product_travel", F.year("end_date_month")).orderBy("n_product_travel", "end_date_month", "last_end_date_month")

prepared_policys = (
    df_policies
    .filter((F.col("end_date").isNotNull()) & (F.col("start_date") >= F.lit("2024-12-02").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_add(F.col("end_date"), 7).alias("end_date_plus_7d"),
    )
)

ranked = (
    prepared_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "end_date_plus_7d",
        F.max("premium").over(w_holder_id_end_date_month).alias("holder_id_end_date_month_max_premium"),
        F.nth_value("end_date_plus_7d", 2).over(w_status_holder_id_by_premium).alias("second_end_date_plus_7d"),
        F.first("start_date", ignorenulls=True).over(w_status_holder_id_by_premium_2).alias("first_known_start_date"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_end_date_month), 2).alias("premium_vs_holder_id_end_date_month_avg"),
        F.rank().over(w_status_holder_id_by_premium_3).alias("premium_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_end_date_month), F.lit(0)), 4).alias("premium_share_of_holder_id_end_date_month"),
        F.lead("end_date_plus_7d").over(w_all_by_end_date_plus_7d).alias("next_end_date_plus_7d"),
        F.size(F.collect_set("product").over(w_holder_id_end_date_month)).alias("distinct_product_in_holder_id_end_date_month"),
        F.last("end_date").over(w_status_holder_id_by_premium).alias("last_end_date"),
        F.last("start_date").over(w_end_date_plus_7d_month_produ).alias("last_start_date"),
        F.round(F.cume_dist().over(w_status_holder_id_by_premium_3), 3).alias("premium_cume_dist"),
    )
)

totals = (
    ranked
    .groupBy(
        F.date_trunc("month", "end_date").alias("end_date_month"),
        F.date_trunc("month", "last_end_date").alias("last_end_date_month"),
    )
    .agg(
        F.sum(F.when(F.col("status").isin("CANCELLED", "LAPSED", "ACTIVE"), F.col("premium_cume_dist")).otherwise(0)).alias("premium_cume_dist_status_in_cancelled_lapsed_a"),
        F.max("holder_id_end_date_month_max_premium").alias("max_holder_id_end_date_month_max_premium"),
        F.sum(F.when(F.col("product") == "TRAVEL", 1).otherwise(0)).alias("n_product_travel"),
        F.sum("distinct_product_in_holder_id_end_date_month").alias("sum_distinct_product_in_holder_id_end_date_month"),
    )
    .filter(F.col("sum_distinct_product_in_holder_id_end_date_month") > 3)
    .drop("sum_distinct_product_in_holder_id_end_date_month")
)

ranked_2 = (
    totals
    .select(
        "*",
        F.round(F.col("n_product_travel") / F.nullif(F.sum("n_product_travel").over(w_end_date_month_month), F.lit(0)), 4).alias("n_product_travel_share_of_end_date_month_month"),
        F.round(F.avg("n_product_travel").over(w_end_date_month_month), 2).alias("end_date_month_month_avg_n_product_travel"),
        F.ntile(3).over(w_n_product_travel_end_date_mo).alias("n_product_travel_tercile"),
    )
)

result = (
    ranked_2
    .filter(F.col("premium_cume_dist_status_in_cancelled_lapsed_a").between(0.8, 4.5))
    .select(
        "end_date_month",
        "last_end_date_month",
        "premium_cume_dist_status_in_cancelled_lapsed_a",
        "max_holder_id_end_date_month_max_premium",
        "n_product_travel",
        "n_product_travel_share_of_end_date_month_month",
        "n_product_travel_tercile",
        F.date_format(F.col("last_end_date_month"), "MMM yyyy").alias("last_end_date_month_label"),
    )
    .orderBy(
        F.col("last_end_date_month_label").desc_nulls_first(),
        F.col("last_end_date_month").desc_nulls_first(),
    )
)
