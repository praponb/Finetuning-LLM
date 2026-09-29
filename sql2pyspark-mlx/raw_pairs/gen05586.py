from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("signup_date", "id")

with_windows = (
    df_customers
    .filter(F.col("segment") != "Corporate")
    .withColumn("next_age", F.lead("age").over(w_running))
    .withColumn("prev_age", F.lag("age").over(w_running))
    .withColumn("prev_credit_limit", F.lag("credit_limit").over(w_running))
    .withColumn("next_credit_limit", F.lead("credit_limit").over(w_running))
    .withColumn("event_seq", F.row_number().over(w_running))
)

result = (
    with_windows
    .select("id", "is_active", "next_age", "next_credit_limit", "event_seq")
    .orderBy(F.col("id").desc_nulls_first(), "event_seq")
)
