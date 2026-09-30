from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .pivot("account_type", ["LOAN", "CHECKING", "SAVINGS"])
    .agg(F.max("balance").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "customer_id",
        F.col("LOAN_total").alias("loan_total"),
        F.col("LOAN_cnt").alias("loan_cnt"),
        F.col("CHECKING_total").alias("checking_total"),
        F.col("CHECKING_cnt").alias("checking_cnt"),
        F.col("SAVINGS_total").alias("savings_total"),
        F.col("SAVINGS_cnt").alias("savings_cnt"),
    )
)
