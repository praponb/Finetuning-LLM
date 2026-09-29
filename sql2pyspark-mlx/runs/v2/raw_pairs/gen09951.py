from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNotNull())
    .select(
        "is_active",
        F.when(F.col("credit_limit") >= 17000, "High").when(F.col("credit_limit") >= 3100, "Medium").otherwise("Low").alias("credit_limit_level"),
        F.concat_ws("/", "country", "name").alias("country_name_key"),
    )
    .orderBy("credit_limit_level")
)
