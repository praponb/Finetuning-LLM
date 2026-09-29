from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

enriched = (
    df_sessions
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

result = (
    enriched
    .select("id", "device", "browser", "started_at", "country", "seq_no")
    .orderBy(F.col("seq_no").desc())
)
