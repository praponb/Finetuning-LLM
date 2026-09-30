from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "TH") & (F.col("segment") != "Corporate"))
    .groupBy(
        "city",
        F.when(F.col("age") >= 58, "high").when(F.col("age") >= 25, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.count("*").alias("num_customers"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        (F.max("age") - F.min("age")).alias("age_range"),
    )
    .orderBy(F.col("age_range").desc())
)
