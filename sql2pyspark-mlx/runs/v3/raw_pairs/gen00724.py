from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") != "SG") & F.col("is_active"))
    .groupBy("is_active", "country")
    .agg(
        F.countDistinct("credit_limit").alias("unique_countrys"),
        F.min("age").alias("min_age"),
    )
    .orderBy("country")
)
