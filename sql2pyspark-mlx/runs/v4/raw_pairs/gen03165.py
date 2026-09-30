from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.lower(F.trim("name")) == "carol diaz") | (F.col("credit_limit") < 11000))
    .select(
        "email",
        F.round((F.col("credit_limit") * 1.07), 2).alias("credit_limit_scaled"),
    )
)
