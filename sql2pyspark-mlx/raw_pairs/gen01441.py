from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id")
w2 = Window.partitionBy("segment")
w3 = Window.partitionBy("country", "city")
w4 = Window.partitionBy(F.year("signup_date"), "city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("age", "id")
w6 = Window.partitionBy("week_start").orderBy(F.col("segment_sum_credit_limit_p50").desc())

with_windows = (
    df_customers
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("segment_sum_credit_limit", F.sum("credit_limit").over(w2))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w3)), 4))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

result = (
    with_windows
    .groupBy(F.date_trunc("WEEK", "signup_date").alias("week_start"), "country")
    .agg(
        F.percentile_approx("segment_sum_credit_limit", 0.5).alias("segment_sum_credit_limit_p50"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.count("*").alias("row_count"),
    )
    .withColumn("segment_sum_credit_limit_p50_rank", F.rank().over(w6))
    .orderBy(F.col("week_start").asc_nulls_last())
)
