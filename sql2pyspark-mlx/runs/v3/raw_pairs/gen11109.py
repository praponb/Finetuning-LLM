from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id")

result = (
    df_customers
    .filter(F.upper(F.trim("email")) == "EMMA@OUTLOOK.COM")
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .orderBy(F.col("credit_limit").desc())
)
