from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("monthly_fee", "id")
w3 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_last())
w5 = Window.partitionBy("plan").orderBy("end_date", "id")

enriched = (
    df_subscriptions
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("cumulative_monthly_fee"),
        F.row_number().over(w2).alias("row_seq"),
        F.sum("monthly_fee").over(w3).alias("cumulative_monthly_fee_2"),
        F.rank().over(w4).alias("monthly_fee_rank"),
        F.lag("monthly_fee", 1, 0).over(w5).alias("prev_monthly_fee"),
    )
)

result = (
    enriched
    .withColumn("week_start", F.date_trunc("WEEK", "start_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "start_date"))
    .cube("week_start", "quarter_start")
    .agg(
        F.sum("row_seq").alias("total_row_seq"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("cumulative_monthly_fee")).otherwise(0)).alias("expired_cumulative_monthly_fee"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("cnt")
)
