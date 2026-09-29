from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date = Window.orderBy("end_date", "id")
w_holder_id_by_start_date_full = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_full = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status = Window.partitionBy("status")
w_end_date_month_by_end_date_f = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_product = Window.partitionBy("status", "product")

valid = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("TRAVEL", "LIFE"), "group_a").when(F.col("product") == "AUTO", "group_b").otherwise("other").alias("product_group"),
    )
)

windowed = (
    valid
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "product_group",
        F.lead("end_date", 2).over(w_all_by_end_date).alias("next2_end_date"),
        F.nth_value("premium", 2).over(w_holder_id_by_start_date_full).alias("second_premium"),
        F.nth_value("premium", 2).over(w_all_by_premium_full).alias("second_premium_2"),
        F.min("premium").over(w_status).alias("status_min_premium"),
        F.last("start_date").over(w_end_date_month_by_end_date_f).alias("last_start_date"),
        F.first("start_date").over(w_end_date_month_by_end_date_f).alias("first_start_date"),
        F.count(F.lit(1)).over(w_status_product).alias("rows_in_status_product"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
    )
)

tagged = (
    windowed
    .filter(F.col("next2_end_date") < F.lit("2026-06-18").cast("date"))
    .select(
        "*",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

result = (
    tagged
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "product_group",
        "second_premium",
        "second_premium_2",
        "status_min_premium",
        "last_start_date",
        "premium_share_of_status",
        "status_or_unknown",
        F.date_trunc("month", F.col("next2_end_date")).alias("next2_end_date_month"),
    )
    .orderBy("end_date", "id")
    .limit(5)
)
