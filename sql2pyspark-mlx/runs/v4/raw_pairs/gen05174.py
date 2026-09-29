from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .withColumn("age_avg_6_prior", F.avg("age").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w2))
)
