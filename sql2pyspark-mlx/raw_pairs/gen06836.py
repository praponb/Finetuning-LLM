from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("method") != "promptpay") & (F.col("paid_at") < "2024-02-18"))
)

paid_cohort_summary = (
    base
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
)

result = paid_cohort_summary.select("paid_cohort", "amount_count", "method_list")
