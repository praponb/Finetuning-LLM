from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") >= 33) & (F.col("signup_date") <= "2023-11-01") & (F.col("credit_limit") > 13700))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.sum("age").alias("total_age"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
    )
    .filter(F.col("total_age") < 350)
)
