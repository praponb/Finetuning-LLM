from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product = Window.partitionBy("holder_id", "product")
w_holder_region_by_start_date = Window.partitionBy("holder_region").orderBy(F.col("start_date").desc(), "id")
w_end_date_month_holder_id_by = Window.partitionBy(F.month("end_date"), "holder_id").orderBy(F.col("premium_filled").desc(), "id")
w_holder_region = Window.partitionBy("holder_region")
w_end_date_month_by_premium_fi = Window.partitionBy(F.month("end_date")).orderBy("premium_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_month_by_premium_fi_2 = Window.partitionBy(F.month("end_date")).orderBy("premium_filled", "id")
w_end_date_month = Window.partitionBy(F.month("end_date"))
w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_end_date_month_holder_id_by_2 = Window.partitionBy(F.month("end_date"), "holder_id").orderBy(F.col("premium_filled").desc())
w_product_group_status_by_end = Window.partitionBy("product_group", "status").orderBy(F.col("end_date").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_holder_id_distinct_product_i = Window.partitionBy("holder_id", "distinct_product_in_holder_region")
w_next2_end_date_month_by_prem = Window.partitionBy(F.date_trunc("month", "next2_end_date")).orderBy("premium_filled_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_product_in_holder_r = Window.partitionBy("distinct_product_in_holder_region", F.month("start_date")).orderBy("start_date_quintile", "id")

cleaned = (
    df_policies
    .filter(F.col("status").isin("ACTIVE", "CANCELLED", "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("HOME", "TRAVEL"), "group_a").when(F.col("product") == "AUTO", "group_b").otherwise("other").alias("product_group"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select("s.*", F.col("p.region").alias("holder_region"))
)

windowed = (
    joined
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "premium_filled",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_product), F.lit(0)), 4).alias("premium_share_of_holder_id_product"),
        F.ntile(5).over(w_holder_region_by_start_date).alias("start_date_quintile"),
        F.ntile(5).over(w_end_date_month_holder_id_by).alias("premium_filled_quintile"),
        F.size(F.collect_set("product_group").over(w_holder_id_product)).alias("distinct_product_group_in_holder_id_product"),
        F.size(F.collect_set("product").over(w_holder_region)).alias("distinct_product_in_holder_region"),
        F.nth_value("start_date", 2).over(w_end_date_month_by_premium_fi).alias("second_start_date"),
        F.lead("end_date", 3).over(w_end_date_month_by_premium_fi_2).alias("next3_end_date"),
        F.round(F.avg("premium").over(w_holder_region), 2).alias("holder_region_avg_premium"),
        F.round(F.col("premium_filled") / F.nullif(F.sum("premium_filled").over(w_end_date_month), F.lit(0)), 4).alias("premium_filled_share_of_end_date_month"),
        (F.col("premium_filled") - F.lag("premium_filled").over(w_holder_region_by_start_date)).alias("premium_filled_change"),
        F.sum("premium_filled").over(w_end_date_month).alias("end_date_month_sum_premium_filled"),
        (F.col("premium_filled") - F.lag("premium_filled").over(w_product_by_premium)).alias("premium_filled_change_2"),
        F.count(F.lit(1)).over(w_holder_id_product).alias("rows_in_holder_id_product"),
        F.lead("end_date", 2).over(w_end_date_month_holder_id_by).alias("next2_end_date"),
        F.round(F.cume_dist().over(w_end_date_month_holder_id_by_2), 3).alias("premium_filled_cume_dist"),
        F.sum("premium").over(w_product_group_status_by_end).alias("premium_sum_6_prior"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.size(F.collect_set("product").over(w_holder_id_distinct_product_i)).alias("distinct_product_in_holder_id_distinct_product_i"),
        F.min("start_date_quintile").over(w_holder_id_distinct_product_i).alias("holder_id_distinct_product_in_holder_region_min_"),
        F.sum("premium_filled_cume_dist").over(w_holder_id_distinct_product_i).alias("holder_id_distinct_product_in_holder_region_sum_"),
        F.sum("holder_region_avg_premium").over(w_next2_end_date_month_by_prem).alias("running_holder_region_avg_premium"),
        F.sum("premium_filled_quintile").over(w_next2_end_date_month_by_prem).alias("running_premium_filled_quintile"),
        F.sum("premium_filled").over(w_next2_end_date_month_by_prem).alias("running_premium_filled"),
        F.row_number().over(w_distinct_product_in_holder_r).alias("seq_no"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "premium_filled",
        "premium_share_of_holder_id_product",
        "distinct_product_group_in_holder_id_product",
        "distinct_product_in_holder_region",
        "second_start_date",
        "next3_end_date",
        "holder_region_avg_premium",
        "premium_filled_share_of_end_date_month",
        "premium_filled_change",
        "end_date_month_sum_premium_filled",
        "premium_filled_change_2",
        "rows_in_holder_id_product",
        "next2_end_date",
        "premium_filled_cume_dist",
        "premium_sum_6_prior",
        "distinct_product_in_holder_id_distinct_product_i",
        "running_holder_region_avg_premium",
        "running_premium_filled_quintile",
        "running_premium_filled",
        "seq_no",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("second_start_date")).alias("days_since_second_start_date"),
    )
)
