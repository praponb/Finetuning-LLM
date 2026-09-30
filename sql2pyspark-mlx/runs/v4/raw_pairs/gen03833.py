from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status")
w_part = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("monthly_fee").desc())

enriched = (
    df_subscriptions
    .select(
        "*",
        F.max("monthly_fee").over(w).alias("plan_max_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w_part).alias("last_known_monthly_fee"),
        F.percent_rank().over(w_hist).alias("monthly_fee_percent_rank"),
    )
)

result = (
    enriched
    .cube(
        F.when(F.col("monthly_fee_percent_rank") >= 0.71, "HIGH").otherwise("LOW").alias("monthly_fee_percent_rank_tier"),
        "plan",
    )
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee_percent_rank")).otherwise(0)).alias("active_monthly_fee_percent_rank"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .filter(F.col("active_monthly_fee_percent_rank") >= 0)
    .orderBy(F.col("enterprise_rows").desc_nulls_first())
)
