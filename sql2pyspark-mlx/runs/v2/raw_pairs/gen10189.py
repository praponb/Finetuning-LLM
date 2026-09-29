from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "LOAN", "SAVINGS"]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .orderBy("opened_date")
)
