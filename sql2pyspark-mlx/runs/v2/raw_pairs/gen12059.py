from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(31, 50))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.countDistinct("age").alias("unique_citys"),
        F.countDistinct("credit_limit").alias("unique_countrys"),
    )
)
