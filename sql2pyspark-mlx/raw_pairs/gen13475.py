from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("status", "plan")
w3 = Window.partitionBy(F.date_trunc("week", "end_date"), "status").orderBy(F.col("monthly_fee").desc(), "id")

scored = (
    df_subscriptions
    .select(
        "*",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_5"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.ntile(10).over(w3).alias("monthly_fee_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("monthly_fee_share") >= 0.74, "HIGH").otherwise("LOW").alias("monthly_fee_share_tier"),
        F.date_trunc("WEEK", "start_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.round(F.sum("monthly_fee_share") / F.nullif(F.count(F.when(F.col("plan") == "standard", 1)), F.lit(0)), 2).alias("monthly_fee_share_per_standard"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_rows"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_rows"),
    )
    .filter(F.col("premium_rows") >= 1)
    .orderBy(F.col("expired_rows").desc())
)
