from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Bangkok", "Singapore"]))
    .select(
        "email",
        F.initcap(F.trim("name")).alias("name_title"),
        F.lower("segment").alias("segment_lower"),
    )
)
