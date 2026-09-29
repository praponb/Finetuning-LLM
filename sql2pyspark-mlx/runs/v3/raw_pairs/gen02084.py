from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") == "SMB") & ~F.col("is_active"))
    .groupBy("is_active")
    .agg(F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"))
    .orderBy(F.col("stddev_credit_limit").desc())
)
