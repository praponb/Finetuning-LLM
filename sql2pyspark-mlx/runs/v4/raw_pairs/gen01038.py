from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "Chiang Mai", "Bangkok"]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
    .orderBy(F.col("country").desc())
)
