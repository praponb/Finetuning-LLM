from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-03-01", "2023-04-01"))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .orderBy("id")
)
