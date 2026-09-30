from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country", F.year("signup_date").alias("signup_year"))
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .filter(F.col("max_credit_limit") < 13000)
    .orderBy("signup_year")
)
