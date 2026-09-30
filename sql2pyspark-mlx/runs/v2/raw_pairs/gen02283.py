from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.quarter("signup_date").alias("signup_quarter"),
        F.year("signup_date").alias("signup_year"),
    )
    .agg(F.countDistinct("credit_limit").alias("unique_segments"))
    .filter(F.col("unique_segments") >= 5)
    .select("signup_quarter", "signup_year", "unique_segments")
)
