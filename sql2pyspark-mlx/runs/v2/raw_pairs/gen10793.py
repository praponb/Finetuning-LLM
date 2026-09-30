from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") != "UPS")
    .groupBy("carrier")
    .agg(
        F.max("cost").alias("max_cost"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("max_cost") < 42)
)
