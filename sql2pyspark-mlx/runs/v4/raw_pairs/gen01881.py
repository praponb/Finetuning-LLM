from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("segment")

result = (
    df_customers
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w_running))
    .withColumn("corr_credit_limit_age_2", F.corr("credit_limit", "age").over(w_running))
)
