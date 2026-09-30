from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNotNull() & ~F.col("name").like("Al%"))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("credit_limit")).otherwise(0)).alias("new_york_credit_limit"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)
