from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-04-01", "2025-07-29"))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "account_type", "opened_date", "customer_id")
)
