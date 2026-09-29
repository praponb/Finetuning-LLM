from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc_nulls_last(), "id")

filtered = (
    df_accounts
    .filter(F.col("status") != "OPEN")
    .withColumn("status_filled", F.coalesce("status", F.lit("Unknown")))
    .withColumn("resolved_status", F.coalesce("status", F.lit("DIRECT")))
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
