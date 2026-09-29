from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "status", "opened_date")
    .orderBy(F.col("opened_date").desc(), F.col("status").desc())
)
