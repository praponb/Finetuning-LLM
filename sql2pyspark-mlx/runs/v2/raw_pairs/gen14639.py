from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") != 61) & F.col("email").like("%@company.co"))
    .select(
        "id",
        "country",
        "age",
        "city",
        F.round((F.col("credit_limit") * 0.85), 2).alias("credit_limit_adjusted"),
        F.lower("city").alias("city_lower"),
    )
)
