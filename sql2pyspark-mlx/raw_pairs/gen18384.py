from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("corporate_events").desc())

result = (
    df_customers
    .groupBy(
        F.date_trunc("YEAR", "signup_date").alias("year_start"),
        F.when(F.col("age") >= 62, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
        "country",
    )
    .agg(
        F.max(F.when(F.col("segment") == "Corporate", F.col("age"))).alias("corporate_max_age"),
        F.count_if(F.col("segment") == "Corporate").alias("corporate_events"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("age"))).alias("consumer_total_age"),
        F.percentile_approx("age", 0.9).alias("age_p90"),
    )
    .withColumn("corporate_events_percent_rank", F.percent_rank().over(w))
    .orderBy("corporate_events")
)
