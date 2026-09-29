from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("segment").orderBy("credit_limit", "id")
w4 = Window.partitionBy("city", "country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city")

metrics = (
    df_customers
    .select(
        "*",
        F.ntile(4).over(w).alias("credit_limit_bucket"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.ntile(5).over(w3).alias("credit_limit_bucket_2"),
        F.sum("age").over(w4).alias("cumulative_age"),
        F.corr("credit_limit", "age").over(w5).alias("corr_credit_limit_age"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.avg(F.when(F.col("segment") == "Corporate", F.col("cumulative_age"))).alias("corporate_avg_cumulative_age"),
        F.max("credit_limit_bucket_2").alias("max_credit_limit_bucket_2"),
    )
    .orderBy(F.col("year_start").desc())
)
