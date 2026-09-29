from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("age_p50").desc())

result = (
    df_customers
    .groupBy(F.date_trunc("QUARTER", "signup_date").alias("quarter_start"), "city")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.bit_or("age").alias("age_bit_or"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
        F.percentile_approx("age", 0.5).alias("age_p50"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .filter(F.col("corr_credit_limit_age") < 1000000)
    .withColumn("age_p50_percent_rank", F.percent_rank().over(w))
)
