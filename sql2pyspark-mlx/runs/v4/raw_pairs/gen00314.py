from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("balance").between(56000, 78000) & (F.col("opened_date") > "2023-05-28"))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
    )
    .filter(F.col("pct_closed") >= 74)
    .orderBy(F.col("opened_quarter").desc())
)
