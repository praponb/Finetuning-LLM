from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(F.col("account_id").isin([1, 39]))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

agg = (
    src
    .groupBy("merchant")
    .agg(F.min("is_flagged_flag").alias("min_is_flagged_flag"))
)

result = (
    agg
    .filter(F.col("min_is_flagged_flag") > 76)
    .select("merchant", "min_is_flagged_flag")
    .orderBy(F.col("merchant").desc())
)
