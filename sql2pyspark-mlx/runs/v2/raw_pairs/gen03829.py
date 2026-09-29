from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "status", "opened_date", "customer_id")
    .orderBy(F.col("customer_id").desc())
)
