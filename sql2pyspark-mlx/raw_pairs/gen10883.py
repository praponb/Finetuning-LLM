from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") > 57) & (F.length("email") > 10) & (F.col("credit_limit") > 7700))
    .select(
        "id",
        "age",
        "segment",
        "signup_date",
        F.round((F.col("credit_limit") * 1.1), 2).alias("credit_limit_adjusted"),
    )
)
