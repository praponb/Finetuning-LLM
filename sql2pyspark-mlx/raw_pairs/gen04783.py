from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "basic"]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .orderBy("id", "start_date")
)
