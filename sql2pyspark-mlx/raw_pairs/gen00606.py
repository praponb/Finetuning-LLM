from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount")

result = (
    df_payments
    .select("id", "method", "amount", F.percent_rank().over(w).alias("amount_pct_rank"))
)
