from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .filter(F.col("amount_range") >= 45)
    .select("txn_cohort", "amount_range", "merchant_list")
    .orderBy("txn_cohort")
)
