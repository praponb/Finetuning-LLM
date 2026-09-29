from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w_recent))
    .withColumn("first_known_browser_2", F.first("browser", ignorenulls=True).over(w_running))
    .withColumn("first_known_browser_3", F.first("browser", ignorenulls=True).over(w_recent))
)
