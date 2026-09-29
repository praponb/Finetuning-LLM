from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "segment", "credit_limit", "signup_date")
    .orderBy(F.col("id").desc(), "credit_limit")
)
