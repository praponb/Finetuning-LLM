from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(44, 46))
    .groupBy(
        "country",
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.count("*").alias("num_customers"),
        F.min("age").alias("min_age"),
    )
    .orderBy("country")
)
