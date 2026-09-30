from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id")

latest = (
    df_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .withColumn("age_tier", F.when(F.col("age") >= 58, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["activity_month", "age_tier"], ["activity_month"], []],
        "activity_month",
        "age_tier",
    )
    .agg(
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit")).otherwise(0)).alias("corporate_credit_limit"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("age")).otherwise(0)).alias("corporate_age"),
        F.grouping("activity_month").alias("grouping_id"),
    )
    .orderBy(F.col("age_tier").desc())
)
