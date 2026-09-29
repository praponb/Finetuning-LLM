from pyspark.sql import functions as F

base = df_transactions.filter(F.col("amount") > 1700)

is_flagged_summary = (
    base
    .groupBy("is_flagged")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    is_flagged_summary
    .filter(F.col("unique_merchants") >= 4)
    .orderBy(F.col("unique_merchants").desc(), "is_flagged")
    .limit(3)
)
