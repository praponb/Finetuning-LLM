from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id")
w2 = Window.partitionBy("segment", "country").orderBy(F.col("signup_date").desc(), "id")
w3 = Window.orderBy(F.col("age").desc(), "id")
w4 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("age_bucket", F.ntile(4).over(w3))
    .withColumn("cumulative_age", F.sum("age").over(w4))
)
