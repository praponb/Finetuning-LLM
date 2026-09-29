from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")

result = (
    df_customers
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .orderBy("country", "city")
)
