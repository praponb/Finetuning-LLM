from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "status", "end_date", "start_date")
)
