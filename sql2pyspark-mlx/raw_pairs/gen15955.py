from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
