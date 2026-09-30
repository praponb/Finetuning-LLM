from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "email",
        "age",
        "city",
        (F.col("credit_limit") + F.col("age")).alias("credit_limit_plus_age"),
        F.when(F.col("segment") == "Consumer", 1).otherwise(0).alias("is_consumer"),
    )
    .orderBy(F.col("id").desc())
)
