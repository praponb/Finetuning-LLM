from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")

result = (
    df_accounts
    .select("id", "account_type", "balance", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "account_type", "balance")
    .orderBy(F.col("balance").desc(), F.col("id").desc())
)
