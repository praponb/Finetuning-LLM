from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNull())
    .groupBy("segment", F.year("signup_date").alias("signup_year"))
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.countDistinct("credit_limit").alias("unique_countrys"),
    )
    .filter(F.col("age_range") >= 26)
    .orderBy("signup_year")
)
