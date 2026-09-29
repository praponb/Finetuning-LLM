from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Grab"]) & (F.col("amount") < 2500))
)

summary = (
    recent_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = summary.select("txn_cohort", "merchant_list", "amount_range")
