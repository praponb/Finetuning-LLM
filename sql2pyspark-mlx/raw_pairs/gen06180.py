from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.col("city") == "Bangkok")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "segment", "credit_limit")
)
