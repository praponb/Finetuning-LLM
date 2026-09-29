from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-08-01", "2025-03-01"))
    .withColumn("prev_balance", F.lag("balance").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)
