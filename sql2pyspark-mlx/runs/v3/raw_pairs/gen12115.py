from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .select("id", "status", "opened_date", "customer_id", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .orderBy("rn", F.col("opened_date").desc())
)
