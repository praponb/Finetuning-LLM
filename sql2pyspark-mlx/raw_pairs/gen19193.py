from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Rossi%") | ~F.col("segment").isin(["Corporate", "Consumer"]))
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.min("age").alias("min_age"),
        F.round(((100 * F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0))) / F.count("*")), 1).alias("pct_smb"),
    )
)
