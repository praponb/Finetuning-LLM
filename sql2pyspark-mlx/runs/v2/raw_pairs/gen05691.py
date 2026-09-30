from pyspark.sql import functions as F

result = (
    df_customers
    .filter((~F.col("is_active") & F.col("credit_limit").between(5700, 7400)) | F.col("email").like("%@startup.io"))
    .select("is_active", F.substring("segment", 1, 2).alias("segment_prefix"))
)
