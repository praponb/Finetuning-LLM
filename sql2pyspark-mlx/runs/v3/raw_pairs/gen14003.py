from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") == "SMB") & (F.lower(F.trim("name")) == "emma brown") & (F.length("email") > 6))
    .select(
        "country",
        "city",
        "name",
        F.least("credit_limit", "age").alias("least_credit_limit_age"),
    )
    .orderBy("name", "country")
)
