from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("balance") <= 68000)
    .select("id", "status", "opened_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "status", "opened_date")
    .orderBy("status", F.col("opened_date").desc())
)
