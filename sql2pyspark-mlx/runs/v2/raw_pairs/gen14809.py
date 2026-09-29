from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select(
        "id",
        "city",
        "age",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
    )
    .orderBy("city", F.col("age").desc())
)
