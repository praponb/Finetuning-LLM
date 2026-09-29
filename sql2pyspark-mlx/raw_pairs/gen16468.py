from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_cohort")

result = (
    df_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy(F.col("cnt").desc())
)
