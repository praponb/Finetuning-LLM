from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"), "city")
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
)
