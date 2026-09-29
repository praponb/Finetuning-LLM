from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 1)
)
