from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") <= "2023-03-12") | F.col("credit_limit").between(6100, 7200))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        "is_active",
    )
    .agg(F.count("*").alias("row_count"))
)
