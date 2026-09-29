from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
    .orderBy(F.col("browser").desc())
)
