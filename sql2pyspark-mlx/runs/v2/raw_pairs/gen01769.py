from pyspark.sql import functions as F

base = df_customers.filter(F.length("name") >= 7)

summary = (
    base
    .groupBy("segment", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
)

result = (
    summary
    .filter(F.col("avg_credit_limit") > 74)
    .orderBy("segment")
)
