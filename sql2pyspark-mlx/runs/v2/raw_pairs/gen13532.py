from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(9700, 11700) & (F.length("name") < 8))
    .groupBy(F.year("signup_date").alias("signup_year"), "segment")
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
)
