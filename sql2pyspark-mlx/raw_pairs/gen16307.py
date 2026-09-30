from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy("signup_month", F.col("avg_credit_limit").desc())
)
