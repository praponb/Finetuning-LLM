from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "segment", "signup_date")
)
