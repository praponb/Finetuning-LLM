from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED"]))
    .groupBy("customer_id")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.count("*").alias("num_accounts"),
    )
)
