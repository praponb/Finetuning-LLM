from pyspark.sql import functions as F

src = df_transactions.filter((F.col("txn_type") == "DEBIT") & ~F.col("is_flagged"))

summary = (
    src
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.sum("amount").alias("total_amount"),
    )
)

result = summary.select("txn_cohort", "merchant_values")
