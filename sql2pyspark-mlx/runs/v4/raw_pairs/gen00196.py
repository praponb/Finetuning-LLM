from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("credit_limit")).otherwise(0)).alias("consumer_credit_limit"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
