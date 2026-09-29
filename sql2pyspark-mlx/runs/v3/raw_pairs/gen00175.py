from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
)
