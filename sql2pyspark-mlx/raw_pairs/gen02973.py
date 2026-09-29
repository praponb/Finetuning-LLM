from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.count("*").alias("num_customers"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
