from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("credit_limit") < 10400)
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "segment", "signup_date")
    .orderBy(F.col("id").desc())
)
