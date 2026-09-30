from pyspark.sql import functions as F

src = df_transactions.filter(F.col("is_flagged"))

is_flagged_summary = (
    src
    .groupBy("is_flagged", "merchant")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    is_flagged_summary
    .select("is_flagged", "merchant", "total_amount")
    .orderBy(F.col("total_amount").desc())
)
