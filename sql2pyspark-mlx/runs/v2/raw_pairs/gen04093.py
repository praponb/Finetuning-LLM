from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") == "SMB") & (F.col("credit_limit") >= 15800))
    .groupBy(
        F.month("signup_date").alias("signup_month"),
        F.year("signup_date").alias("signup_year"),
    )
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.avg("age").alias("avg_age"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("avg_age") < 66)
)
