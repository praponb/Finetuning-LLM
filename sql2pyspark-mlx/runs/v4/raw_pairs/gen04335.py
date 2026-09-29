from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 2)
    .orderBy("status", "opened_date")
)
