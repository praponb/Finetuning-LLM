from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w_grp = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")
w_part = Window.partitionBy("plan").orderBy("monthly_fee_avg_5", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_subscriptions
    .filter(F.month("end_date").isin([3, 7, 9]))
    .select(
        "*",
        F.concat("status", F.lit(" "), "plan").alias("status_plan"),
        F.when(F.col("status").isin(["PAUSED", "EXPIRED"]), "Priority").otherwise("Other").alias("status_group"),
    )
)

scored = (
    base
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status_plan",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.avg("monthly_fee").over(w_grp).alias("monthly_fee_avg_5"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.sum("monthly_fee").over(w_part).alias("last4_sum_monthly_fee"),
        F.avg("monthly_fee").over(w_part).alias("last4_avg_monthly_fee"),
        F.count("monthly_fee").over(w_part).alias("last4_count_monthly_fee"),
    )
)

grouped = (
    enriched
    .cube(
        F.when(F.col("last4_avg_monthly_fee") >= 230, "HIGH").otherwise("LOW").alias("last4_avg_monthly_fee_tier"),
        "plan",
    )
    .agg(
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("monthly_fee_per_premium"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
    )
)

result = (
    grouped
    .select(
        "plan",
        "standard_count",
        "distinct_ids",
        "enterprise_count",
        "basic_events",
    )
    .orderBy(F.col("distinct_ids").desc())
)
