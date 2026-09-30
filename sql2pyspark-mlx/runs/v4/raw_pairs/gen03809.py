from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "segment")
    .agg(
        F.sum("age").alias("total_age"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("total_age") > 140)
)
