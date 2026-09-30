from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .filter(F.month("signup_date").isin([2, 4, 5]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 1)
)
