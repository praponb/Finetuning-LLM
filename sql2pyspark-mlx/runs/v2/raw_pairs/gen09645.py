from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .select("id", "customer_id", "opened_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .orderBy("id")
)
