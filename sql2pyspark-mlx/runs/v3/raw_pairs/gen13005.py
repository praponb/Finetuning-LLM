from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 2)
    .orderBy(F.col("rn").desc(), "plan")
)
