from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .filter(F.col("avg_credit_limit") >= 46)
    .select("signup_month", "avg_credit_limit", "segment_list")
)
