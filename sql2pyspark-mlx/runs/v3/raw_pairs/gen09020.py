from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["CANCELLED", "ACTIVE"]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "plan", "end_date", "customer_id")
)
