from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .withColumn("running_sum_credit_limit", F.sum("credit_limit").over(w))
    .withColumn("running_avg_credit_limit", F.avg("credit_limit").over(w))
    .withColumn("running_min_credit_limit", F.min("credit_limit").over(w))
    .withColumn("running_max_credit_limit", F.max("credit_limit").over(w))
)
