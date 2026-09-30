from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id")

result = (
    df_customers
    .filter(F.col("signup_date") <= "2024-04-17")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .orderBy(F.col("signup_date").desc())
)
