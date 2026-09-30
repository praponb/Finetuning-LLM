from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status_by_premium = Window.partitionBy("holder_id", "status").orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_status_by_premium_2 = Window.partitionBy("holder_id", "status").orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product = Window.partitionBy("product")
w_status_product_by_second_pre = Window.partitionBy("status", "product").orderBy("second_premium_filled", "id")
w_end_date_month = Window.partitionBy(F.date_trunc("month", "end_date"))
w_status_product_by_second_pre_2 = Window.partitionBy("status", "product").orderBy("second_premium_filled")
w_status_product_by_second_pre_3 = Window.partitionBy("status", "product").orderBy("second_premium_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_policies
    .filter((F.col("holder_id") <= 19) & (F.col("product").isin("HOME", "TRAVEL")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.date_add(F.col("start_date"), 30).alias("start_date_plus_30d"),
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
    )
)

sequenced = (
    prepared
    .select(
        "*",
        F.nth_value("premium_filled", 2).over(w_holder_id_status_by_premium).alias("second_premium_filled"),
        F.sum("premium").over(w_holder_id_status_by_premium_2).alias("running_premium"),
        F.min("premium_filled").over(w_product).alias("product_min_premium_filled"),
        F.last("premium", ignorenulls=True).over(w_holder_id_status_by_premium_2).alias("last_known_premium"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        "premium_filled",
        "start_date_plus_30d",
        "second_premium_filled",
        "running_premium",
        "product_min_premium_filled",
        "last_known_premium",
        F.lead("start_date", 2).over(w_status_product_by_second_pre).alias("next2_start_date"),
        F.count(F.lit(1)).over(w_end_date_month).alias("rows_in_end_date_month"),
        F.round(F.cume_dist().over(w_status_product_by_second_pre_2), 3).alias("second_premium_filled_cume_dist"),
        F.first("last_known_premium", ignorenulls=True).over(w_status_product_by_second_pre_3).alias("first_known_last_known_premium"),
    )
)

result = (
    with_trends
    .cube("holder_id", "product", "status")
    .agg(F.array_sort(F.collect_set("status")).alias("status_list"))
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
