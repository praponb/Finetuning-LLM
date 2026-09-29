from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .select("id", "customer_id", "opened_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "customer_id", "opened_date")
    .orderBy(F.col("customer_id").desc())
)
