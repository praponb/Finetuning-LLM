from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

valid = (
    df_sessions
    .filter(F.col("browser") == "Edge")
    .withColumn("is_tablet", F.when(F.col("device") == "tablet", 1).otherwise(0))
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
