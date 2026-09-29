from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 9400)
    .select(
        "id",
        "is_active",
        "signup_date",
        F.when(F.col("country") == "JP", 1).otherwise(0).alias("is_jp"),
        F.substring("name", 1, 2).alias("name_prefix"),
    )
    .orderBy("name_prefix")
)
