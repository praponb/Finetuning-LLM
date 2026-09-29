from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")

result = (
    df_transactions
    .select("id", "merchant", "amount", F.ntile(4).over(w).alias("bucket"))
    .orderBy("bucket")
)
