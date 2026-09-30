from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .orderBy("avg_credit_limit")
)
