from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "age",
        "email",
        "country",
        F.when(F.col("credit_limit") >= 15100, "High").when(F.col("credit_limit") >= 4800, "Medium").otherwise("Low").alias("credit_limit_tier"),
    )
    .orderBy("email")
)
