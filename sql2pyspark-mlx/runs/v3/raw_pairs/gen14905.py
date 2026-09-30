from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .select("id", "status", "opened_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "status", "opened_date")
    .orderBy(F.col("id").desc())
)
