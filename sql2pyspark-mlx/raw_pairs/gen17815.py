from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("activity_month", F.date_trunc("MONTH", "order_date"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1580, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_date"))
    .groupingSets(
        [["activity_month", "amount_tier", "quarter_start"], ["activity_month", "amount_tier"]],
        "activity_month",
        "amount_tier",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
    )
    .orderBy(F.col("amount_p95").desc())
)
