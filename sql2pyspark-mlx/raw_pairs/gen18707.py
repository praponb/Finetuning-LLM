from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("order_id").isNull() | (F.col("amount") > 1090)))
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(F.max("paid_at").alias("last_paid_at"))
)
