from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("country").isin(["SG", "JP", "UK"]) & (F.col("segment") == "Corporate") & ~F.col("country").isin(["SG", "TH", "JP"]))
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.count("*").alias("cnt"),
    )
    .orderBy("cnt")
)
