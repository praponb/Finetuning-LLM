from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .select("id", "status", "opened_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .orderBy("opened_date")
)
