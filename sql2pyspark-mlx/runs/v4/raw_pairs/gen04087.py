from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w2))
)
