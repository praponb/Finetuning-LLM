from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNull() & F.col("is_active"))
    .groupBy("city")
    .agg(
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("credit_limit_range") > 66)
)
