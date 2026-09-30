from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(9, 13) & (F.to_date("paid_at") >= "2023-03-04"))
    .groupBy("status")
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") > 5)
    .orderBy(F.col("unique_statuss").desc(), "status")
    .limit(3)
)
