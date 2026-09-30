from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 65, "high").when(F.col("age") >= 35, "mid").otherwise("low").alias("age_bucket"),
        F.year("signup_date").alias("signup_year"),
    )
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("min_credit_limit") > 5600)
)
