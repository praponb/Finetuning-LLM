from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.col("credit_limit").between(4800, 9200))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .orderBy("email")
)
