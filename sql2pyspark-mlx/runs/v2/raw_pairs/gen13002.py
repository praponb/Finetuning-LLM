from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.year("signup_date") == 2025) & (F.col("country") == "UK") & (F.col("credit_limit") >= 6300))
)

agg = (
    base
    .groupBy("segment")
    .agg(
        F.countDistinct("credit_limit").alias("unique_citys"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
)

result = (
    agg
    .filter(F.col("unique_citys") > 9)
    .orderBy(F.col("credit_limit_range").desc())
)
