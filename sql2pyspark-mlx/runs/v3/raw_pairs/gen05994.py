from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum("age").alias("total_age"),
    )
    .filter(F.col("cnt") < 7)
    .select("signup_year", "cnt", "total_age")
    .orderBy(F.col("total_age").desc())
)
