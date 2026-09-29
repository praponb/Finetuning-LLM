from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([1, 2, 5]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "customer_id", "end_date")
)
