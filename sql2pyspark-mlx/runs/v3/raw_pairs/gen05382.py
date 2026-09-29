from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 140)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "customer_id", "end_date")
    .orderBy(F.col("customer_id").desc())
)
