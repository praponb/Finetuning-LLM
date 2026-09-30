from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.month("opened_date").isin([2, 9, 11]))
    .select("id", "status", "opened_date", F.row_number().over(w).alias("row_num"))
)
