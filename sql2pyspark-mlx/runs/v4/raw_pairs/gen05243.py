from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("account_type", "status").orderBy(F.col("opened_date").desc(), "id")
w4 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_type", "status")

result = (
    df_accounts
    .withColumn("next_balance", F.lead("balance", 1, 0).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w4))
    .withColumn("account_type_max_balance", F.max("balance").over(w5))
)
