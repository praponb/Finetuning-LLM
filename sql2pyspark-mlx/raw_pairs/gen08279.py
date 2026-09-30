from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy(F.year("start_date")).orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy(F.col("last7_min_monthly_fee").desc_nulls_first(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("last7_min_monthly_fee").desc_nulls_last())

enriched = (
    df_subscriptions
    .filter(F.col("end_date") > "2024-12-01")
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        F.min("monthly_fee").over(w_grp).alias("last7_min_monthly_fee"),
        F.max("monthly_fee").over(w_grp).alias("last7_max_monthly_fee"),
        F.avg("monthly_fee").over(w_grp).alias("last7_avg_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped_subscriptions = (
    deduped
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
)

base = (
    deduped_subscriptions
    .filter(F.col("start_date") > F.lit("2023-02-24").cast("date"))
    .select(
        "*",
        F.round((F.col("running_monthly_fee") * 100), 2).alias("running_monthly_fee_scaled"),
    )
)

result = (
    base
    .groupBy(
        "plan",
        "status",
        F.when(F.col("running_monthly_fee") >= 1242, "HIGH").otherwise("LOW").alias("running_monthly_fee_tier"),
    )
    .agg(
        F.min("last7_max_monthly_fee").alias("min_last7_max_monthly_fee"),
        F.sum(F.when(F.col("status") == "PAUSED", F.col("running_monthly_fee")).otherwise(0)).alias("paused_running_monthly_fee"),
        F.round(F.sum("running_monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("running_monthly_fee_per_premium"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
        F.percentile_approx("running_monthly_fee", 0.9).alias("running_monthly_fee_p90"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_rows"),
    )
    .filter(F.col("standard_count") < 1000)
)
