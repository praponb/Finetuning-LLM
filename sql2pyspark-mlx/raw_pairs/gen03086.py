from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_cohort")

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy(F.col("total_amount").desc())
)
