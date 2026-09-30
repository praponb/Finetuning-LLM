from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id", "plan").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.length("status").alias("status_length"))
)

base = (
    cleaned
    .filter(F.col("end_date") > F.lit("2023-08-24").cast("date"))
    .select("*", F.col("monthly_fee").cast("int").alias("monthly_fee_int"))
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    deduped
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w2))
    .withColumn("last_known_status_length", F.last("status_length", ignorenulls=True).over(w_recent))
)

deduped_subscriptions = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    deduped_subscriptions
    .groupBy("customer_id", "status")
    .agg(
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee_int")).otherwise(0)).alias("standard_monthly_fee_int"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("last_known_monthly_fee")).otherwise(0)).alias("enterprise_last_known_monthly_fee"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("enterprise_last_known_monthly_fee") >= 0)
    .orderBy("enterprise_events")
)
