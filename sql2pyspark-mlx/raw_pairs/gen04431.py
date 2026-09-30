from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date")).orderBy("signup_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("is_active_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("MONTH", "signup_date"), "segment")
w5 = Window.partitionBy("city")
w_grp = Window.partitionBy("segment").orderBy(F.col("city_stddev_samp_age").desc(), "id")
w7 = Window.partitionBy("city", "country").orderBy("is_active_flag", "id")
w_part = Window.partitionBy("country", "city").orderBy("is_active_flag", "id")
w9 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("city").orderBy("prev_is_active_flag_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("country", "segment").orderBy("last_known_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy(F.date_trunc("MONTH", "signup_date")).orderBy("city_stddev_samp_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "*",
        F.col("is_active").cast("int").alias("is_active_flag"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

with_windows = (
    filtered
    .withColumn("is_active_flag_avg_6", F.avg("is_active_flag").over(w))
    .withColumn("first_known_segment", F.first("segment", ignorenulls=True).over(w2))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w3))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w4))
    .withColumn("city_stddev_samp_age", F.stddev_samp("age").over(w5))
)

with_windows_customers = (
    with_windows
    .filter(F.col("city_stddev_samp_age") == 65)
    .select(
        "*",
        F.row_number().over(w_grp).alias("row_seq"),
        F.row_number().over(w7).alias("event_seq"),
        F.lag("is_active_flag_avg_6", 1).over(w_part).alias("prev_is_active_flag_avg_6"),
    )
)

enriched = (
    with_windows_customers
    .withColumn("last_known_credit_limit", F.last("credit_limit", ignorenulls=True).over(w9))
    .withColumn("running_corr_age_credit_limit", F.sum("corr_age_credit_limit").over(w10))
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("row_seq").over(w11).alias("running_sum_row_seq"),
        F.min("row_seq").over(w11).alias("running_min_row_seq"),
        F.avg("row_seq").over(w11).alias("running_avg_row_seq"),
        F.first("is_active_flag_avg_6", ignorenulls=True).over(w12).alias("first_known_is_active_flag_avg_6"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("running_min_row_seq") >= 8.4, "HIGH").otherwise("LOW").alias("running_min_row_seq_tier"),
        "segment",
    )
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.avg(F.when(F.col("country") == "TH", F.col("credit_limit"))).alias("th_avg_credit_limit"),
    )
)
