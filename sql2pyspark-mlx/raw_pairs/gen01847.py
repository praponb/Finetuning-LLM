from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_customers
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        F.avg("age").over(w_running).alias("running_avg_age"),
        F.sum("age").over(w_running).alias("running_sum_age"),
        F.count("age").over(w_running).alias("running_count_age"),
    )
)

result = (
    scored
    .select("id", "email", "running_avg_age", "running_sum_age")
    .orderBy(F.col("running_avg_age").desc())
)
