from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") != "JP")
    .select(
        "credit_limit",
        F.initcap(F.trim("name")).alias("name_title"),
        F.floor("credit_limit").alias("credit_limit_whole"),
    )
)
