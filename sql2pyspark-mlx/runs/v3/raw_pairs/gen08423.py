from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "age",
        F.round((F.col("credit_limit") * 1.15), 2).alias("credit_limit_scaled"),
    )
    .orderBy(F.col("age").desc())
)
