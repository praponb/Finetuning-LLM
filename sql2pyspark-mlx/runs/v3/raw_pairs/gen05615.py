from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 5)
)
