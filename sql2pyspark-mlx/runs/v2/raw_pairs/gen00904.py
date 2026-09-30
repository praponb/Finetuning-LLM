from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
)
