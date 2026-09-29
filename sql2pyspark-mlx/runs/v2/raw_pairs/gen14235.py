from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .select("id", "status", "opened_date", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "status", "opened_date")
    .orderBy(F.col("id").desc())
)
