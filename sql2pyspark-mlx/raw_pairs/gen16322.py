from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .orderBy("account_type", F.col("customer_id").desc())
)
