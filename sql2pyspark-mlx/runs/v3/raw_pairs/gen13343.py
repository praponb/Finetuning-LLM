from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2023-04-12")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "customer_id", "balance")
)
