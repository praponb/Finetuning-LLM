from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .filter(F.year("opened_date") >= 2024)
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .orderBy("opened_date", F.col("rn").desc())
)
