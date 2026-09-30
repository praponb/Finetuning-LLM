from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CREDIT", "CHECKING"]))
    .select("id", "account_type", "balance", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("id").desc())
)
