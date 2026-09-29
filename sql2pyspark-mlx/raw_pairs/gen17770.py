from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").asc_nulls_last(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("end_date") > F.lit("2023-06-05").cast("date")))
    .withColumn("days_since_end_date", F.datediff(F.current_date(), "end_date"))
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "plan", "end_date", "status")
)
