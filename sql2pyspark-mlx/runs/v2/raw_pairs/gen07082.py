from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") > 12400) & (F.col("credit_limit") > 10600))
    .select(
        "id",
        "name",
        "credit_limit",
        "city",
        F.lower("country").alias("country_lower"),
    )
    .orderBy(F.col("id").desc())
)
