from pyspark.sql import functions as F

src = (
    df_customers
    .filter((F.col("segment") != "SMB") & (F.col("age") > 41) & (F.col("credit_limit") < 8100))
)

customers_stats = (
    src
    .groupBy("city")
    .agg(
        F.count("*").alias("row_count"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
)

result = (
    customers_stats
    .filter(F.col("row_count") < 5)
    .orderBy("credit_limit_range", "city")
    .limit(50)
)
