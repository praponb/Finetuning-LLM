from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "status", "end_date", "monthly_fee")
    .orderBy(F.col("status").desc())
)
