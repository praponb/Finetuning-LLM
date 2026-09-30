from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")

latest = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("status").rlike("^(EXPIRED|PAUSED)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy("status")
)
