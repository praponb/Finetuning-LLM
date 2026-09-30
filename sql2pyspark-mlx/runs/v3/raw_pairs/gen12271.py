from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("credit_limit") < 11300) & ((F.col("signup_date") > "2025-01-01") | F.col("segment").isin(["Corporate", "SMB"])))
)

summary = (
    base
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"), "segment")
    .agg(F.countDistinct("credit_limit").alias("unique_countrys"))
)

result = (
    summary
    .filter(F.col("unique_countrys") > 5)
    .select("signup_month", "segment")
    .orderBy(F.col("segment").desc())
)
