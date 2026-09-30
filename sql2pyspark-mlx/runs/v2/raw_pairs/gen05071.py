from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .select(
        "id",
        "name",
        "email",
        F.when(F.col("segment") == "Consumer", 1).otherwise(0).alias("is_consumer"),
        F.length("email").alias("email_length"),
    )
)
