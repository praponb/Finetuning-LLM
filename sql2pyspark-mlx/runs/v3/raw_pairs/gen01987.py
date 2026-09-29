from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("balance") >= 37000)
    .select("id", "status", "opened_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .orderBy("opened_date")
)
