from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_region_status = Window.partitionBy("holder_region", "status")
w_status_by_holder_joined_on = Window.partitionBy("status").orderBy("holder_joined_on", "id")
w_all_by_premium_moving_6 = Window.orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_premium_running = Window.orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium = Window.orderBy(F.col("premium").asc_nulls_last(), "id")
w_status = Window.partitionBy("status")
w_status_holder_region_by_hold = Window.partitionBy("status", "holder_region").orderBy(F.col("holder_joined_on").desc_nulls_first(), "id")

cleaned = df_policies.filter(F.col("product").isin("HOME", "AUTO", "HEALTH"))

with_parent = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.region") == "South")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.size(F.collect_set("holder_id").over(w_holder_region_status)).alias("distinct_holder_id_in_holder_region_status"),
        F.rank().over(w_status_by_holder_joined_on).alias("holder_joined_on_rank"),
        F.sum("premium").over(w_all_by_premium_moving_6).alias("premium_sum_7"),
        F.min("premium").over(w_holder_region_status).alias("holder_region_status_min_premium"),
        F.first("start_date", ignorenulls=True).over(w_all_by_premium_running).alias("first_known_start_date"),
        F.lead("end_date", 2).over(w_all_by_premium).alias("next2_end_date"),
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
        F.ntile(3).over(w_all_by_premium).alias("premium_tercile"),
        F.lag("start_date").over(w_status_holder_region_by_hold).alias("prev_start_date"),
    )
)

result = (
    ranked
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        "holder_joined_on",
        "distinct_holder_id_in_holder_region_status",
        "premium_sum_7",
        "holder_region_status_min_premium",
        "first_known_start_date",
        "premium_tercile",
    )
    .orderBy("premium_sum_7")
)
