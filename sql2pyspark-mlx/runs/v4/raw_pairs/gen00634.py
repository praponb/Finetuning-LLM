from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("started_at").desc(), "id")
w5 = Window.partitionBy("browser", "customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)
