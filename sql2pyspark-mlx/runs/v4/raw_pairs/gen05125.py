from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(~F.col("is_flagged") & (~F.col("is_flagged") | F.col("account_id").isin([21, 25, 31, 36])))
)

agg = (
    src
    .groupBy("merchant")
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.count("*").alias("cnt"),
    )
)

result = (
    agg
    .filter(F.col("cnt") >= 6)
    .select("merchant", "cnt")
    .orderBy("merchant", F.col("cnt").desc())
)
