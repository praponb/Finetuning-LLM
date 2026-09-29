from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("method") == "card"))
    .withColumn("method_title", F.initcap(F.trim("method")))
)

agg = (
    filtered_payments
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(F.min("paid_at").alias("min_paid_at"))
)

result = agg
