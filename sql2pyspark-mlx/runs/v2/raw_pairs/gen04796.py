from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 1)
    .orderBy(F.col("opened_date").desc())
)
