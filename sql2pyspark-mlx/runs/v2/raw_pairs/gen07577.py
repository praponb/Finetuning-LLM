from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date")

result = (
    df_accounts
    .select("id", "status", "opened_date", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .orderBy(F.col("opened_date").desc())
)
