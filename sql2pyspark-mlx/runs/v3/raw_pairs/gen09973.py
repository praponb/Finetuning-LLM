from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.year("opened_date") == 2024)
    .select("id", "account_type", "balance", F.row_number().over(w).alias("row_num"))
)
