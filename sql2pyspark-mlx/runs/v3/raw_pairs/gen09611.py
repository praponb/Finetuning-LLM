from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") < 55) & ((F.col("age") != 35) | F.col("age").between(51, 60)))
    .select(
        "id",
        "country",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
    )
    .orderBy("country", "id")
    .limit(50)
)
