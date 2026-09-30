from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .orderBy("age")
)
