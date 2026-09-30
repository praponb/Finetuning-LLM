from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-05-02", "2025-09-01"))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "status", "monthly_fee")
    .orderBy(F.col("id").desc())
)
