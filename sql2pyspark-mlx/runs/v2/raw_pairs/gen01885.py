from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("country").isin(["DE", "JP"]) & ((F.col("country") == "US") | F.col("city").isin(["Chiang Mai", "New York"])))
    .groupBy("country")
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.count("*").alias("cnt"),
    )
)
