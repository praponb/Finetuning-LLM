from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")

result = (
    df_accounts
    .select("id", "status", "balance", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "status", "balance")
    .orderBy("status")
)
