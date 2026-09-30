from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(75000, 76000))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
    .filter(F.col("pct_open") < 27)
    .select("status", "pct_open")
    .orderBy(F.col("pct_open").desc())
)
