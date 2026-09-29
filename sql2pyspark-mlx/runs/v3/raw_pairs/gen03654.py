from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "region",
        F.when(F.col("sqft") >= 13010, "high").when(F.col("sqft") >= 7326, "mid").otherwise("low").alias("sqft_bucket"),
        F.month("opened_date").alias("opened_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
    )
)
