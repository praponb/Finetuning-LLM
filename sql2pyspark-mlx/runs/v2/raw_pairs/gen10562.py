from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "is_active",
        "email",
        F.substring("country", 1, 3).alias("country_prefix"),
    )
    .orderBy(F.col("id").desc())
)
