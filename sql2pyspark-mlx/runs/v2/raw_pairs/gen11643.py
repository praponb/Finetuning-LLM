from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("total_balance") < 287000)
)
